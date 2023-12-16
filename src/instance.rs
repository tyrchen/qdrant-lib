use crate::{
    helpers::{create_general_purpose_runtime, create_search_runtime, create_update_runtime},
    AliasRequest, AliasResponse, CollectionRequest, CollectionResponse, Handler, PointsRequest,
    PointsResponse, QdrantClient, QdrantError, QdrantMsg, QueryRequest, QueryResponse, Settings,
};
use async_trait::async_trait;
use collection::shards::channel_service::ChannelService;
use serde::{Deserialize, Serialize};
use std::{mem::ManuallyDrop, sync::Arc, thread, time::Duration};
use storage::content_manager::{
    consensus::persistent::Persistent, errors::StorageError, toc::TableOfContent,
};
use tokio::{
    runtime::Handle,
    sync::{mpsc, oneshot},
};
use tracing::{debug, warn};

const QDRANT_CHANNEL_BUFFER: usize = 1024;

#[derive(Debug, Deserialize)]
pub enum QdrantRequest {
    Collection(CollectionRequest),
    Alias(AliasRequest),
    Points(PointsRequest),
    Query(QueryRequest),
}

#[derive(Debug, Serialize)]
pub enum QdrantResponse {
    Collection(CollectionResponse),
    Alias(AliasResponse),
    Points(PointsResponse),
    Query(QueryResponse),
}

pub struct QdrantInstance;

impl QdrantInstance {
    pub fn start(config_path: Option<String>) -> Result<Arc<QdrantClient>, QdrantError> {
        let (tx, mut rx) = mpsc::channel::<QdrantMsg>(QDRANT_CHANNEL_BUFFER);

        let (terminated_tx, terminated_rx) = oneshot::channel::<()>();

        let handle = thread::Builder::new()
            .name("qdrant".to_string())
            .spawn(move || {
                let (toc, rt) = start_qdrant(config_path)?;
                let toc_clone = toc.clone();
                rt.block_on(async move {
                    while let Some((msg, resp_sender)) = rx.recv().await {
                        let toc_clone = toc.clone();
                        tokio::spawn(async move {
                            let res = msg.handle(&toc_clone).await;
                            if let Err(e) = resp_sender.send(res) {
                                warn!("Failed to send response: {:?}", e);
                            }
                        });
                    }
                    Ok::<(), QdrantError>(())
                })?;

                // clean things up
                // see this thread: https://github.com/qdrant/qdrant/issues/1316
                let mut toc_arc = toc_clone;
                loop {
                    match Arc::try_unwrap(toc_arc) {
                        Ok(toc) => {
                            drop(toc);
                            if let Err(e) = terminated_tx.send(()) {
                                warn!("Failed to send termination signal: {:?}", e);
                            }
                            break;
                        }
                        Err(toc) => {
                            toc_arc = toc;
                            warn!("Waiting for ToC to be gracefully dropped");
                            thread::sleep(Duration::from_millis(300));
                        }
                    }
                }
                Ok::<(), QdrantError>(())
            })
            .unwrap();
        Ok(Arc::new(QdrantClient {
            tx: ManuallyDrop::new(tx),
            handle,
            terminated_rx,
        }))
    }
}

#[async_trait]
impl Handler for QdrantRequest {
    type Response = QdrantResponse;
    type Error = StorageError;

    async fn handle(self, toc: &TableOfContent) -> Result<Self::Response, Self::Error> {
        match self {
            QdrantRequest::Collection(req) => {
                let resp = req.handle(toc).await?;
                Ok(QdrantResponse::Collection(resp))
            }
            QdrantRequest::Alias(req) => {
                let resp = req.handle(toc).await?;
                Ok(QdrantResponse::Alias(resp))
            }
            QdrantRequest::Points(req) => {
                let resp = req.handle(toc).await?;
                Ok(QdrantResponse::Points(resp))
            }
            QdrantRequest::Query(req) => {
                let resp = req.handle(toc).await?;
                Ok(QdrantResponse::Query(resp))
            }
        }
    }
}

/// Start Qdrant and get TableOfContent.
fn start_qdrant(config_path: Option<String>) -> Result<(Arc<TableOfContent>, Handle), QdrantError> {
    let settings = Settings::new(config_path).expect("Failed to load settings");

    memory::madvise::set_global(settings.storage.mmap_advice);
    segment::vector_storage::common::set_async_scorer(settings.storage.async_scorer);

    if let Some(recovery_warning) = &settings.storage.recovery_mode {
        warn!("Qdrant is loaded in recovery mode: {}", recovery_warning);
        warn!("Read more: https://qdrant.tech/documentation/guides/administration/#recovery-mode");
    }

    // Saved state of the consensus. This is useless for single node mode.
    let persistent_consensus_state =
        Persistent::load_or_init(&settings.storage.storage_path, true)?;

    // Create and own search runtime out of the scope of async context to ensure correct
    // destruction of it
    let search_runtime = create_search_runtime(settings.storage.performance.max_search_threads)
        .expect("Can't search create runtime.");

    let update_runtime =
        create_update_runtime(settings.storage.performance.max_optimization_threads)
            .expect("Can't optimizer create runtime.");

    let general_runtime =
        create_general_purpose_runtime().expect("Can't optimizer general purpose runtime.");
    let runtime_handle = general_runtime.handle().clone();

    // Channel service is used to manage connections between peers.
    // It allocates required number of channels and manages proper reconnection handling. This is useless for single node mode.
    let channel_service = ChannelService::new(6333);

    // Table of content manages the list of collections.
    // It is a main entry point for the storage.
    let toc = TableOfContent::new(
        &settings.storage,
        search_runtime,
        update_runtime,
        general_runtime,
        channel_service.clone(),
        persistent_consensus_state.this_peer_id(),
        None,
    );

    toc.clear_all_tmp_directories()?;

    // Here we load all stored collections.
    runtime_handle.block_on(async {
        for collection in toc.all_collections().await {
            debug!("Loaded collection: {}", collection);
        }
    });

    Ok((Arc::new(toc), runtime_handle))
}

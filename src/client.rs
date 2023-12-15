use std::{mem::ManuallyDrop, thread};

use crate::{CollectionRequest, CollectionResponse, QdrantClient, QdrantResponse};
use anyhow::Result;
use collection::operations::types::VectorsConfig;
use storage::content_manager::collection_meta_ops::CreateCollection;
use tokio::sync::oneshot::{self, error::TryRecvError};
use tracing::warn;

impl Drop for QdrantClient {
    fn drop(&mut self) {
        // drop the tx channel to terminate the qdrant thread
        unsafe {
            ManuallyDrop::drop(&mut self.tx);
        }
        while let Err(TryRecvError::Empty) = self.terminated_rx.try_recv() {
            warn!("Waiting for qdrant to terminate");
            thread::sleep(std::time::Duration::from_millis(100));
        }
    }
}

impl QdrantClient {
    /// Create a new collection.
    pub async fn create_collection(&self, name: &str, config: VectorsConfig) -> Result<bool> {
        let (tx, rx) = oneshot::channel::<QdrantResponse>();
        let data = CreateCollection {
            vectors: config,
            shard_number: None,
            sharding_method: None,
            replication_factor: None,
            write_consistency_factor: None,
            on_disk_payload: None,
            hnsw_config: None,
            wal_config: None,
            optimizers_config: None,
            init_from: None,
            quantization_config: None,
            sparse_vectors: None,
        };
        let msg = CollectionRequest::Create((name.to_string(), data));
        if let Err(e) = self.tx.send((msg.into(), tx)).await {
            warn!("Failed to send request: {:?}", e);
        }
        match rx.await {
            Ok(QdrantResponse::Collection(CollectionResponse::Create(v))) => Ok(v),
            Err(e) => Err(e.into()),
            res => panic!("Unexpected response: {:?}", res),
        }
    }

    pub async fn list_collections(&self) -> Result<Vec<String>> {
        let (tx, rx) = oneshot::channel::<QdrantResponse>();
        let msg = CollectionRequest::List;
        self.tx.send((msg.into(), tx)).await?;
        let res = rx.await?;
        let res = match res {
            QdrantResponse::Collection(CollectionResponse::List(v)) => v,
            _ => panic!("Unexpected response: {:?}", res),
        };
        let res = res.collections.into_iter().map(|v| v.name).collect();
        Ok(res)
    }
}

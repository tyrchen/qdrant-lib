use crate::{
    CollectionRequest, CollectionResponse, QdrantClient, QdrantError, QdrantMsg, QdrantRequest,
    QdrantResponse, QdrantResult,
};
use collection::operations::types::VectorsConfig;
use std::{mem::ManuallyDrop, thread};
use storage::content_manager::collection_meta_ops::CreateCollection;
use tokio::sync::{
    mpsc,
    oneshot::{self, error::TryRecvError},
};
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
    pub async fn create_collection(
        &self,
        name: &str,
        config: VectorsConfig,
    ) -> Result<bool, QdrantError> {
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
        match send_request(&self.tx, msg.into()).await {
            Ok(QdrantResponse::Collection(CollectionResponse::Create(v))) => Ok(v),

            Err(e) => Err(e),
            res => panic!("Unexpected response: {:?}", res),
        }
    }

    pub async fn list_collections(&self) -> Result<Vec<String>, QdrantError> {
        match send_request(&self.tx, CollectionRequest::List.into()).await {
            Ok(QdrantResponse::Collection(CollectionResponse::List(v))) => {
                let res = v.collections.into_iter().map(|v| v.name).collect();
                Ok(res)
            }
            Err(e) => Err(e),
            res => panic!("Unexpected response: {:?}", res),
        }
    }
}

async fn send_request(
    sender: &mpsc::Sender<QdrantMsg>,
    msg: QdrantRequest,
) -> Result<QdrantResponse, QdrantError> {
    let (tx, rx) = oneshot::channel::<QdrantResult>();
    if let Err(e) = sender.send((msg, tx)).await {
        warn!("Failed to send request: {:?}", e);
    }
    let ret = rx.await?;
    Ok::<_, QdrantError>(ret?)
}

use collection::operations::types::CollectionError;
use storage::content_manager::errors::StorageError;
use thiserror::Error;
use tokio::sync::oneshot;

#[derive(Error, Debug)]
pub enum QdrantError {
    #[error("Collection error: {0}")]
    Collection(#[from] CollectionError),
    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),
    #[error("Response error: {0}")]
    ResponseRecv(#[from] oneshot::error::RecvError),
}

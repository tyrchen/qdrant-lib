mod client;
mod config;
mod error;
mod helpers;
mod instance;
mod ops;

use std::backtrace::Backtrace;
use std::mem::ManuallyDrop;
use std::panic;
use std::thread::JoinHandle;
use storage::content_manager::errors::StorageError;
use storage::content_manager::toc::TableOfContent;
use tokio::sync::{mpsc, oneshot};
use tracing::error;

pub use config::Settings;
pub use error::QdrantError;
pub use instance::QdrantInstance;
pub use instance::{QdrantRequest, QdrantResponse};
pub use ops::*;

type QdrantMsg = (QdrantRequest, QdrantResponder);
type QdrantResult = Result<QdrantResponse, StorageError>;
type QdrantResponder = oneshot::Sender<QdrantResult>;

#[derive(Debug)]
pub struct QdrantClient {
    tx: ManuallyDrop<mpsc::Sender<QdrantMsg>>,
    terminated_rx: oneshot::Receiver<()>,
    #[allow(dead_code)]
    handle: JoinHandle<Result<(), QdrantError>>,
}

#[async_trait::async_trait]
trait Handler {
    type Response;
    type Error;
    async fn handle(self, toc: &TableOfContent) -> Result<Self::Response, Self::Error>;
}

pub fn setup_panic_hook() {
    panic::set_hook(Box::new(move |panic_info| {
        let backtrace = Backtrace::force_capture().to_string();
        let loc = if let Some(loc) = panic_info.location() {
            format!(" in file {} at line {}", loc.file(), loc.line())
        } else {
            String::new()
        };
        let message = if let Some(s) = panic_info.payload().downcast_ref::<&str>() {
            s
        } else if let Some(s) = panic_info.payload().downcast_ref::<String>() {
            s
        } else {
            "Payload not captured as it is not a string."
        };

        error!("Panic backtrace: \n{}", backtrace);
        error!("Panic occurred{loc}: {message}");
    }));
}

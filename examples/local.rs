use anyhow::Result;
use qdrant_lib::{QdrantError, QdrantInstance};
use storage::content_manager::errors::StorageError;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let client = QdrantInstance::start(None)?;
    let collection_name = "test_collection2";
    match client
        .create_collection(collection_name, Default::default())
        .await
    {
        Ok(v) => println!("Collection created: {:?}", v),
        Err(QdrantError::Storage(StorageError::BadInput { description })) => {
            println!("{description}");
        }
        Err(e) => panic!("Unexpected error: {:?}", e),
    }

    let collections = client.list_collections().await?;
    println!("Collections: {:?}", collections);

    Ok(())
}

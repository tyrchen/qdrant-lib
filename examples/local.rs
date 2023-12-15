use anyhow::Result;
use qdrant_lib::QdrantInstance;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let client = QdrantInstance::start(None)?;
    let collection_name = "test_collection";
    let _ = client
        .create_collection(collection_name, Default::default())
        .await?;

    let collections = client.list_collections().await?;
    println!("Collections: {:?}", collections);

    Ok(())
}

use anyhow::Result;
use qdrant_lib::QdrantInstance;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let client = QdrantInstance::start(None)?;
    let collection_name = "test_collection2";
    let ret = client
        .create_collection(collection_name, Default::default())
        .await?;

    println!("Collection created: {:?}", ret);

    let collections = client.list_collections().await?;
    println!("Collections: {:?}", collections);

    Ok(())
}

use anyhow::Result;
use collection::operations::types::{SearchRequest, SearchRequestInternal};
use llm_sdk::{EmbeddingRequest, LlmSdk};
use qdrant_lib::QdrantInstance;
use segment::types::WithPayloadInterface;
use std::env;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

const COLLECTION_NAME: &str = "wikipedia";

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "indexer=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let client = QdrantInstance::start(None)?;

    let sdk = LlmSdk::new(env::var("OPENAI_API_KEY")?);
    let embeddings = sdk
        .embedding(EmbeddingRequest::new("What is the capital of France?"))
        .await?
        .data
        .pop()
        .unwrap()
        .embedding;

    let data = SearchRequest {
        search_request: SearchRequestInternal {
            vector: embeddings.into(),
            filter: None,
            with_payload: Some(WithPayloadInterface::Bool(true)),
            with_vector: None,
            offset: None,
            limit: 10,
            score_threshold: None,
            params: Default::default(),
        },
        shard_key: None,
    };

    let ret = client.search_points(COLLECTION_NAME, data).await?;
    println!("Search result: {:#?}", ret);
    Ok(())
}

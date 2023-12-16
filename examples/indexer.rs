use std::{
    fs::File,
    io::{BufRead, BufReader},
    mem,
    num::NonZeroU64,
};

use anyhow::Result;
use collection::operations::{point_ops::PointStruct, types::VectorParams};
use qdrant_lib::QdrantInstance;
use segment::types::{Distance, Payload};
use serde_json::{json, Value};
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use zip::ZipArchive;

const OPENAI_EMBEDDING_DIM: u64 = 1536;
const COLLECTION_NAME: &str = "wikipedia";
const BATCH_SIZE: usize = 8 * 1024;

struct EmbeddingItem {
    id: u64,
    doc: String,
    embedding: Vec<f32>,
}

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
    client.delete_collection(COLLECTION_NAME).await?;

    let params = VectorParams {
        size: NonZeroU64::new(OPENAI_EMBEDDING_DIM).unwrap(),
        distance: Distance::Cosine,
        hnsw_config: None,
        quantization_config: None,
        on_disk: Some(true),
    };
    client
        .create_collection(COLLECTION_NAME, params.into())
        .await?;

    let filename = "./fixtures/wikipedia.zip";

    info!("Loading embeddings from {}", filename);

    let mut archive = ZipArchive::new(File::open(filename)?)?;
    let file = archive.by_index(0)?;
    let reader = BufReader::new(file);
    let mut total = 0usize;
    let mut batch = Vec::with_capacity(BATCH_SIZE);
    for line in reader.lines() {
        let data: Vec<Value> = serde_json::from_str(&line?)?;
        let doc = data[0]["input"].as_str().unwrap().to_string();
        let embedding: Vec<f32> = data[1]["data"][0]["embedding"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_f64().unwrap() as f32)
            .collect();

        total += 1;
        let point: PointStruct = EmbeddingItem::new(total as _, doc, embedding).into();
        batch.push(point);
        if total % BATCH_SIZE == 0 {
            client
                .upsert_points(COLLECTION_NAME, mem::take(&mut batch))
                .await?;
            info!("Loaded {} embeddings", total);
        }
    }

    if !batch.is_empty() {
        client
            .upsert_points(COLLECTION_NAME, mem::take(&mut batch))
            .await?;
        info!("Loaded {} embeddings", total);
    }

    let ret = client.count_points(COLLECTION_NAME, None, true).await?;
    info!("Total points: {}", ret);

    Ok(())
}

impl EmbeddingItem {
    fn new(id: u64, doc: String, embedding: Vec<f32>) -> Self {
        Self { id, doc, embedding }
    }
}

impl From<EmbeddingItem> for PointStruct {
    fn from(item: EmbeddingItem) -> Self {
        let payload: Payload = json!({
            "doc": item.doc,
        })
        .into();

        PointStruct {
            id: item.id.into(),
            vector: item.embedding.into(),
            payload: Some(payload),
        }
    }
}

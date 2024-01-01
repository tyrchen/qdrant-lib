use crate::{
    AliasRequest, AliasResponse, ColName, CollectionRequest, CollectionResponse, PointsRequest,
    PointsResponse, QdrantClient, QdrantError, QdrantMsg, QdrantRequest, QdrantResponse,
    QdrantResult, QueryRequest, QueryResponse,
};
use collection::operations::{
    payload_ops::{DeletePayload, SetPayload},
    point_ops::{PointStruct, PointsSelector},
    types::{
        CollectionError, CollectionInfo, CountRequest, CountRequestInternal, PointGroup,
        PointRequest, RecommendGroupsRequest, RecommendRequest, RecommendRequestBatch, Record,
        SearchGroupsRequest, SearchRequest, SearchRequestBatch, UpdateResult, VectorsConfig,
    },
    vector_ops::{DeleteVectors, PointVectors, UpdateVectors},
};
use segment::types::{Filter, ScoredPoint};
use std::{mem::ManuallyDrop, thread};
use storage::content_manager::collection_meta_ops::{CreateCollection, UpdateCollection};
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
        name: impl Into<String>,
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

        let msg = CollectionRequest::Create((name.into(), data));
        match send_request(&self.tx, msg.into()).await {
            Ok(QdrantResponse::Collection(CollectionResponse::Create(v))) => Ok(v),

            Err(e) => Err(e),
            res => panic!("Unexpected response: {:?}", res),
        }
    }

    /// List all collections.
    pub async fn list_collections(&self) -> Result<Vec<String>, QdrantError> {
        match send_request(&self.tx, CollectionRequest::List.into()).await {
            Ok(QdrantResponse::Collection(CollectionResponse::List(v))) => Ok(v),
            Err(e) => Err(e),
            res => panic!("Unexpected response: {:?}", res),
        }
    }

    /// Get collection info by name.
    pub async fn get_collection(
        &self,
        name: impl Into<String>,
    ) -> Result<Option<CollectionInfo>, QdrantError> {
        match send_request(&self.tx, CollectionRequest::Get(name.into()).into()).await {
            Ok(QdrantResponse::Collection(CollectionResponse::Get(v))) => Ok(Some(v)),
            Err(QdrantError::Collection(CollectionError::NotFound { .. })) => Ok(None),
            Err(e) => Err(e),
            res => panic!("Unexpected response: {:?}", res),
        }
    }

    /// Update collection info by name.
    pub async fn update_collection(
        &self,
        name: impl Into<String>,
        data: UpdateCollection,
    ) -> Result<bool, QdrantError> {
        let msg = CollectionRequest::Update((name.into(), data));
        match send_request(&self.tx, msg.into()).await {
            Ok(QdrantResponse::Collection(CollectionResponse::Update(v))) => Ok(v),
            Err(e) => Err(e),
            res => panic!("Unexpected response: {:?}", res),
        }
    }

    /// Delete collection by name.
    pub async fn delete_collection(&self, name: impl Into<String>) -> Result<bool, QdrantError> {
        match send_request(&self.tx, CollectionRequest::Delete(name.into()).into()).await {
            Ok(QdrantResponse::Collection(CollectionResponse::Delete(v))) => Ok(v),
            Err(e) => Err(e),
            res => panic!("Unexpected response: {:?}", res),
        }
    }

    /// Create alias for collection.
    pub async fn create_alias(
        &self,
        collection_name: impl Into<String>,
        alias_name: impl Into<String>,
    ) -> Result<bool, QdrantError> {
        let msg = AliasRequest::Create((collection_name.into(), alias_name.into()));
        match send_request(&self.tx, msg.into()).await {
            Ok(QdrantResponse::Alias(AliasResponse::Create(v))) => Ok(v),
            Err(e) => Err(e),
            res => panic!("Unexpected response: {:?}", res),
        }
    }

    /// List all aliases.
    pub async fn list_aliases(&self) -> Result<Vec<(ColName, String)>, QdrantError> {
        match send_request(&self.tx, AliasRequest::List.into()).await {
            Ok(QdrantResponse::Alias(AliasResponse::List(v))) => {
                let res = v
                    .aliases
                    .into_iter()
                    .map(|v| (v.collection_name, v.alias_name))
                    .collect();
                Ok(res)
            }
            Err(e) => Err(e),
            res => panic!("Unexpected response: {:?}", res),
        }
    }

    /// Get aliases for collection.
    pub async fn get_aliases(
        &self,
        collection_name: impl Into<String>,
    ) -> Result<Vec<(ColName, String)>, QdrantError> {
        match send_request(&self.tx, AliasRequest::Get(collection_name.into()).into()).await {
            Ok(QdrantResponse::Alias(AliasResponse::Get(v))) => {
                let res = v
                    .aliases
                    .into_iter()
                    .map(|v| (v.collection_name, v.alias_name))
                    .collect();
                Ok(res)
            }
            Err(e) => Err(e),
            res => panic!("Unexpected response: {:?}", res),
        }
    }

    /// Delete alias.
    pub async fn delete_alias(&self, alias_name: impl Into<String>) -> Result<bool, QdrantError> {
        let msg = AliasRequest::Delete(alias_name.into());
        match send_request(&self.tx, msg.into()).await {
            Ok(QdrantResponse::Alias(AliasResponse::Delete(v))) => Ok(v),
            Err(e) => Err(e),
            res => panic!("Unexpected response: {:?}", res),
        }
    }

    /// Rename alias.
    pub async fn rename_alias(
        &self,
        old_alias_name: impl Into<String>,
        new_alias_name: impl Into<String>,
    ) -> Result<bool, QdrantError> {
        let msg = AliasRequest::Rename((old_alias_name.into(), new_alias_name.into()));
        match send_request(&self.tx, msg.into()).await {
            Ok(QdrantResponse::Alias(AliasResponse::Rename(v))) => Ok(v),
            Err(e) => Err(e),
            res => panic!("Unexpected response: {:?}", res),
        }
    }

    /// get points from collection
    pub async fn get_points(
        &self,
        collection_name: impl Into<String>,
        data: PointRequest,
    ) -> Result<Vec<Record>, QdrantError> {
        let msg = PointsRequest::Get((collection_name.into(), data));
        match send_request(&self.tx, msg.into()).await {
            Ok(QdrantResponse::Points(PointsResponse::Get(v))) => Ok(v),
            Err(e) => Err(e),
            res => panic!("Unexpected response: {:?}", res),
        }
    }

    /// upsert points to collection
    pub async fn upsert_points(
        &self,
        collection_name: impl Into<String>,
        points: Vec<PointStruct>,
    ) -> Result<UpdateResult, QdrantError> {
        let msg = PointsRequest::Upsert((collection_name.into(), points.into()));
        match send_request(&self.tx, msg.into()).await {
            Ok(QdrantResponse::Points(PointsResponse::Upsert(v))) => Ok(v),
            Err(e) => Err(e),
            res => panic!("Unexpected response: {:?}", res),
        }
    }

    /// delete points from collection
    pub async fn delete_points(
        &self,
        collection_name: impl Into<String>,
        points: PointsSelector,
    ) -> Result<UpdateResult, QdrantError> {
        let msg = PointsRequest::Delete((collection_name.into(), points));
        match send_request(&self.tx, msg.into()).await {
            Ok(QdrantResponse::Points(PointsResponse::Delete(v))) => Ok(v),
            Err(e) => Err(e),
            res => panic!("Unexpected response: {:?}", res),
        }
    }

    /// count points in collection
    pub async fn count_points(
        &self,
        collection_name: impl Into<String>,
        filter: Option<Filter>,
        exact: bool,
    ) -> Result<usize, QdrantError> {
        let data = CountRequest {
            count_request: CountRequestInternal { filter, exact },
            shard_key: None,
        };
        let msg = PointsRequest::Count((collection_name.into(), data));
        match send_request(&self.tx, msg.into()).await {
            Ok(QdrantResponse::Points(PointsResponse::Count(v))) => Ok(v.count),
            Err(e) => Err(e),
            res => panic!("Unexpected response: {:?}", res),
        }
    }

    /// update point vectors
    pub async fn update_vectors(
        &self,
        collection_name: impl Into<String>,
        points: Vec<PointVectors>,
    ) -> Result<UpdateResult, QdrantError> {
        let data = UpdateVectors {
            points,
            shard_key: None,
        };
        let msg = PointsRequest::UpdateVectors((collection_name.into(), data));
        match send_request(&self.tx, msg.into()).await {
            Ok(QdrantResponse::Points(PointsResponse::UpdateVectors(v))) => Ok(v),
            Err(e) => Err(e),
            res => panic!("Unexpected response: {:?}", res),
        }
    }

    /// delete point vectors
    pub async fn delete_vectors(
        &self,
        collection_name: impl Into<String>,
        data: DeleteVectors,
    ) -> Result<UpdateResult, QdrantError> {
        let msg = PointsRequest::DeleteVectors((collection_name.into(), data));
        match send_request(&self.tx, msg.into()).await {
            Ok(QdrantResponse::Points(PointsResponse::DeleteVectors(v))) => Ok(v),
            Err(e) => Err(e),
            res => panic!("Unexpected response: {:?}", res),
        }
    }

    /// set point payload
    pub async fn set_payload(
        &self,
        collection_name: impl Into<String>,
        data: SetPayload,
    ) -> Result<UpdateResult, QdrantError> {
        let msg = PointsRequest::SetPayload((collection_name.into(), data));
        match send_request(&self.tx, msg.into()).await {
            Ok(QdrantResponse::Points(PointsResponse::SetPayload(v))) => Ok(v),
            Err(e) => Err(e),
            res => panic!("Unexpected response: {:?}", res),
        }
    }

    /// delete point payload
    pub async fn delete_payload(
        &self,
        collection_name: impl Into<String>,
        data: DeletePayload,
    ) -> Result<UpdateResult, QdrantError> {
        let msg = PointsRequest::DeletePayload((collection_name.into(), data));
        match send_request(&self.tx, msg.into()).await {
            Ok(QdrantResponse::Points(PointsResponse::DeletePayload(v))) => Ok(v),
            Err(e) => Err(e),
            res => panic!("Unexpected response: {:?}", res),
        }
    }

    /// clear point payload
    pub async fn clear_payload(
        &self,
        collection_name: impl Into<String>,
        points: PointsSelector,
    ) -> Result<UpdateResult, QdrantError> {
        let msg = PointsRequest::ClearPayload((collection_name.into(), points));
        match send_request(&self.tx, msg.into()).await {
            Ok(QdrantResponse::Points(PointsResponse::ClearPayload(v))) => Ok(v),
            Err(e) => Err(e),
            res => panic!("Unexpected response: {:?}", res),
        }
    }

    /// search for vectors
    pub async fn search_points(
        &self,
        collection_name: impl Into<String>,
        data: SearchRequest,
    ) -> Result<Vec<ScoredPoint>, QdrantError> {
        let msg = QueryRequest::Search((collection_name.into(), data));
        match send_request(&self.tx, msg.into()).await {
            Ok(QdrantResponse::Query(QueryResponse::Search(v))) => Ok(v),
            Err(e) => Err(e),
            res => panic!("Unexpected response: {:?}", res),
        }
    }

    // search for vectors in batch
    pub async fn search_points_batch(
        &self,
        collection_name: impl Into<String>,
        data: Vec<SearchRequest>,
    ) -> Result<Vec<Vec<ScoredPoint>>, QdrantError> {
        let data = SearchRequestBatch { searches: data };
        let msg = QueryRequest::SearchBatch((collection_name.into(), data));
        match send_request(&self.tx, msg.into()).await {
            Ok(QdrantResponse::Query(QueryResponse::SearchBatch(v))) => Ok(v),
            Err(e) => Err(e),
            res => panic!("Unexpected response: {:?}", res),
        }
    }

    /// search points group by
    pub async fn search_points_group_by(
        &self,
        collection_name: impl Into<String>,
        data: SearchGroupsRequest,
    ) -> Result<Vec<PointGroup>, QdrantError> {
        let msg = QueryRequest::SearchGroup((collection_name.into(), data));
        match send_request(&self.tx, msg.into()).await {
            Ok(QdrantResponse::Query(QueryResponse::SearchGroup(v))) => Ok(v.groups),
            Err(e) => Err(e),
            res => panic!("Unexpected response: {:?}", res),
        }
    }

    /// recommend result
    pub async fn recommend_points(
        &self,
        collection_name: impl Into<String>,
        data: RecommendRequest,
    ) -> Result<Vec<ScoredPoint>, QdrantError> {
        let msg = QueryRequest::Recommend((collection_name.into(), data));
        match send_request(&self.tx, msg.into()).await {
            Ok(QdrantResponse::Query(QueryResponse::Recommend(v))) => Ok(v),
            Err(e) => Err(e),
            res => panic!("Unexpected response: {:?}", res),
        }
    }

    /// recommend batch
    pub async fn recommend_points_batch(
        &self,
        collection_name: impl Into<String>,
        data: Vec<RecommendRequest>,
    ) -> Result<Vec<Vec<ScoredPoint>>, QdrantError> {
        let data = RecommendRequestBatch { searches: data };
        let msg = QueryRequest::RecommendBatch((collection_name.into(), data));
        match send_request(&self.tx, msg.into()).await {
            Ok(QdrantResponse::Query(QueryResponse::RecommendBatch(v))) => Ok(v),
            Err(e) => Err(e),
            res => panic!("Unexpected response: {:?}", res),
        }
    }

    /// recommend group by
    pub async fn recommend_points_group_by(
        &self,
        collection_name: impl Into<String>,
        data: RecommendGroupsRequest,
    ) -> Result<Vec<PointGroup>, QdrantError> {
        let msg = QueryRequest::RecommendGroup((collection_name.into(), data));
        match send_request(&self.tx, msg.into()).await {
            Ok(QdrantResponse::Query(QueryResponse::RecommendGroup(v))) => Ok(v.groups),
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

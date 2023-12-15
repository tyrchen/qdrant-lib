use std::time::Duration;

use super::ColName;
use crate::{Handler, QdrantRequest};
use async_trait::async_trait;
use collection::{
    common::batching::batch_requests,
    operations::{
        consistency_params::ReadConsistency,
        shard_selector_internal::ShardSelectorInternal,
        types::{
            CoreSearchRequest, CoreSearchRequestBatch, GroupsResult, RecommendGroupsRequest,
            RecommendGroupsRequestInternal, RecommendRequest, RecommendRequestBatch,
            SearchGroupsRequest, SearchGroupsRequestInternal, SearchRequest, SearchRequestBatch,
        },
    },
};
use segment::types::ScoredPoint;
use serde::{Deserialize, Serialize};
use storage::content_manager::{errors::StorageError, toc::TableOfContent};

#[derive(Debug, Serialize, Deserialize)]
pub enum QueryRequest {
    /// search for vectors
    Search((ColName, SearchRequest)),
    /// search for vectors in batch
    SearchBatch((ColName, SearchRequestBatch)),
    /// search for groups
    SearchGroup((ColName, SearchGroupsRequest)),
    /// recommend points
    Recommend((ColName, RecommendRequest)),
    /// recommend points in batch
    RecommendBatch((ColName, RecommendRequestBatch)),
    /// recommend for groups
    RecommendGroup((ColName, RecommendGroupsRequest)),
}

#[derive(Debug, Serialize)]
pub enum QueryResponse {
    /// search result
    Search(Vec<ScoredPoint>),
    /// search result in batch
    SearchBatch(Vec<Vec<ScoredPoint>>),
    /// search group result
    SearchGroup(GroupsResult),
    /// recommend result
    Recommend(Vec<ScoredPoint>),
    /// recommend result in batch
    RecommendBatch(Vec<Vec<ScoredPoint>>),
    /// recommend group result
    RecommendGroup(GroupsResult),
}

#[async_trait]
impl Handler for QueryRequest {
    type Response = QueryResponse;
    type Error = StorageError;

    async fn handle(self, toc: &TableOfContent) -> Result<Self::Response, Self::Error> {
        match self {
            QueryRequest::Search((collection_name, request)) => {
                let SearchRequest {
                    search_request,
                    shard_key,
                } = request;

                let shard_selection = match shard_key {
                    None => ShardSelectorInternal::All,
                    Some(shard_keys) => shard_keys.into(),
                };
                let res = do_core_search_points(
                    toc,
                    &collection_name,
                    search_request.into(),
                    None,
                    shard_selection,
                    None,
                )
                .await?;
                Ok(QueryResponse::Search(res))
            }
            QueryRequest::SearchBatch((collection_name, request)) => {
                let requests = request
                    .searches
                    .into_iter()
                    .map(|req| {
                        let SearchRequest {
                            search_request,
                            shard_key,
                        } = req;
                        let shard_selection = match shard_key {
                            None => ShardSelectorInternal::All,
                            Some(shard_keys) => shard_keys.into(),
                        };
                        let core_request: CoreSearchRequest = search_request.into();

                        (core_request, shard_selection)
                    })
                    .collect();

                let res =
                    do_search_batch_points(toc, &collection_name, requests, None, None).await?;
                Ok(QueryResponse::SearchBatch(res))
            }
            QueryRequest::SearchGroup((collection_name, request)) => {
                let SearchGroupsRequest {
                    search_group_request,
                    shard_key,
                } = request;

                let shard_selection = match shard_key {
                    None => ShardSelectorInternal::All,
                    Some(shard_keys) => shard_keys.into(),
                };
                let res = do_search_point_groups(
                    toc,
                    &collection_name,
                    search_group_request.into(),
                    None,
                    shard_selection,
                    None,
                )
                .await?;
                Ok(QueryResponse::SearchGroup(res))
            }
            QueryRequest::Recommend((collection_name, request)) => {
                let RecommendRequest {
                    recommend_request,
                    shard_key,
                } = request;

                let shard_selection = match shard_key {
                    None => ShardSelectorInternal::All,
                    Some(shard_keys) => shard_keys.into(),
                };
                let res = toc
                    .recommend(
                        &collection_name,
                        recommend_request,
                        None,
                        shard_selection,
                        None,
                    )
                    .await?;
                Ok(QueryResponse::Recommend(res))
            }
            QueryRequest::RecommendBatch((collection_name, request)) => {
                let res =
                    do_recommend_batch_points(toc, &collection_name, request, None, None).await?;
                Ok(QueryResponse::RecommendBatch(res))
            }
            QueryRequest::RecommendGroup((collection_name, request)) => {
                let RecommendGroupsRequest {
                    recommend_group_request,
                    shard_key,
                } = request;

                let shard_selection = match shard_key {
                    None => ShardSelectorInternal::All,
                    Some(shard_keys) => shard_keys.into(),
                };
                let res = do_recommend_point_groups(
                    toc,
                    &collection_name,
                    recommend_group_request.into(),
                    None,
                    shard_selection,
                    None,
                )
                .await?;
                Ok(QueryResponse::RecommendGroup(res))
            }
        }
    }
}

impl From<QueryRequest> for QdrantRequest {
    fn from(req: QueryRequest) -> Self {
        QdrantRequest::Query(req)
    }
}

async fn do_core_search_points(
    toc: &TableOfContent,
    collection_name: &str,
    request: CoreSearchRequest,
    read_consistency: Option<ReadConsistency>,
    shard_selection: ShardSelectorInternal,
    timeout: Option<Duration>,
) -> Result<Vec<ScoredPoint>, StorageError> {
    let batch_res = do_core_search_batch_points(
        toc,
        collection_name,
        CoreSearchRequestBatch {
            searches: vec![request],
        },
        read_consistency,
        shard_selection,
        timeout,
    )
    .await?;
    batch_res
        .into_iter()
        .next()
        .ok_or_else(|| StorageError::service_error("Empty search result"))
}

async fn do_search_batch_points(
    toc: &TableOfContent,
    collection_name: &str,
    requests: Vec<(CoreSearchRequest, ShardSelectorInternal)>,
    read_consistency: Option<ReadConsistency>,
    timeout: Option<Duration>,
) -> Result<Vec<Vec<ScoredPoint>>, StorageError> {
    let requests = batch_requests::<
        (CoreSearchRequest, ShardSelectorInternal),
        ShardSelectorInternal,
        Vec<CoreSearchRequest>,
        Vec<_>,
    >(
        requests,
        |(_, shard_selector)| shard_selector,
        |(request, _), core_reqs| {
            core_reqs.push(request);
            Ok(())
        },
        |shard_selector, core_requests, res| {
            if core_requests.is_empty() {
                return Ok(());
            }

            let core_batch = CoreSearchRequestBatch {
                searches: core_requests,
            };

            let req = toc.core_search_batch(
                collection_name,
                core_batch,
                read_consistency,
                shard_selector,
                timeout,
            );
            res.push(req);
            Ok(())
        },
    )?;

    let results = futures::future::try_join_all(requests).await?;
    let flatten_results: Vec<Vec<_>> = results.into_iter().flatten().collect();
    Ok(flatten_results)
}

async fn do_core_search_batch_points(
    toc: &TableOfContent,
    collection_name: &str,
    request: CoreSearchRequestBatch,
    read_consistency: Option<ReadConsistency>,
    shard_selection: ShardSelectorInternal,
    timeout: Option<Duration>,
) -> Result<Vec<Vec<ScoredPoint>>, StorageError> {
    toc.core_search_batch(
        collection_name,
        request,
        read_consistency,
        shard_selection,
        timeout,
    )
    .await
}

async fn do_search_point_groups(
    toc: &TableOfContent,
    collection_name: &str,
    request: SearchGroupsRequestInternal,
    read_consistency: Option<ReadConsistency>,
    shard_selection: ShardSelectorInternal,
    timeout: Option<Duration>,
) -> Result<GroupsResult, StorageError> {
    toc.group(
        collection_name,
        request.into(),
        read_consistency,
        shard_selection,
        timeout,
    )
    .await
}

async fn do_recommend_point_groups(
    toc: &TableOfContent,
    collection_name: &str,
    request: RecommendGroupsRequestInternal,
    read_consistency: Option<ReadConsistency>,
    shard_selection: ShardSelectorInternal,
    timeout: Option<Duration>,
) -> Result<GroupsResult, StorageError> {
    toc.group(
        collection_name,
        request.into(),
        read_consistency,
        shard_selection,
        timeout,
    )
    .await
}

async fn do_recommend_batch_points(
    toc: &TableOfContent,
    collection_name: &str,
    request: RecommendRequestBatch,
    read_consistency: Option<ReadConsistency>,
    timeout: Option<Duration>,
) -> Result<Vec<Vec<ScoredPoint>>, StorageError> {
    let requests = request
        .searches
        .into_iter()
        .map(|req| {
            let shard_selector = match req.shard_key {
                None => ShardSelectorInternal::All,
                Some(shard_key) => ShardSelectorInternal::from(shard_key),
            };

            (req.recommend_request, shard_selector)
        })
        .collect();

    toc.recommend_batch(collection_name, requests, read_consistency, timeout)
        .await
}

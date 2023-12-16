use super::{shard_selector, ColName};
use crate::{Handler, QdrantRequest};
use async_trait::async_trait;
use collection::{
    operations::{
        payload_ops::{DeletePayload, DeletePayloadOp, PayloadOps, SetPayload, SetPayloadOp},
        point_ops::{
            FilterSelector, PointIdsList, PointInsertOperations, PointOperations, PointsSelector,
            WriteOrdering,
        },
        shard_key_selector::ShardKeySelector,
        shard_selector_internal::ShardSelectorInternal,
        types::{CountRequest, CountResult, PointRequest, Record, UpdateResult},
        vector_ops::{DeleteVectors, UpdateVectors, UpdateVectorsOp, VectorOperations},
        CollectionUpdateOperations,
    },
    shards::shard::ShardId,
};
use serde::{Deserialize, Serialize};
use storage::content_manager::{errors::StorageError, toc::TableOfContent};

#[derive(Debug, Deserialize)]
pub enum PointsRequest {
    /// get points with given info
    Get((ColName, PointRequest)),
    /// count points for given collection
    Count((ColName, CountRequest)),
    /// delete points with given info
    Delete((ColName, PointsSelector)),
    /// upsert points with given info
    Upsert((ColName, PointInsertOperations)),
    // update points with given info
    // UpdateBatch((ColName, UpdateOperations)),
    /// update point vectors
    UpdateVectors((ColName, UpdateVectors)),
    /// delete point vectors
    DeleteVectors((ColName, DeleteVectors)),
    /// set point payload
    SetPayload((ColName, SetPayload)),
    /// overwrite point payload
    OverwritePayload((ColName, SetPayload)),
    /// delete point payload
    DeletePayload((ColName, DeletePayload)),
    /// clear point payload
    ClearPayload((ColName, PointsSelector)),
}

#[derive(Debug, Serialize)]
pub enum PointsResponse {
    /// get points result
    Get(Vec<Record>),
    /// count status
    Count(CountResult),
    /// delete status
    Delete(UpdateResult),
    /// upsert status
    Upsert(UpdateResult),
    /// update status
    UpdateVectors(UpdateResult),
    /// delete status
    DeleteVectors(UpdateResult),
    /// set payload status
    SetPayload(UpdateResult),
    /// overwrite payload status
    OverwritePayload(UpdateResult),
    /// delete payload status
    DeletePayload(UpdateResult),
    /// clear payload status
    ClearPayload(UpdateResult),
}

#[async_trait]
impl Handler for PointsRequest {
    type Response = PointsResponse;
    type Error = StorageError;

    async fn handle(self, toc: &TableOfContent) -> Result<Self::Response, Self::Error> {
        match self {
            PointsRequest::Get((col_name, request)) => {
                let PointRequest {
                    point_request,
                    shard_key,
                } = request;

                let shard = shard_selector(shard_key);
                let ret = toc.retrieve(&col_name, point_request, None, shard).await?;
                Ok(PointsResponse::Get(ret))
            }
            PointsRequest::Count((col_name, request)) => {
                let CountRequest {
                    count_request,
                    shard_key,
                } = request;

                let shard = shard_selector(shard_key);
                let ret = toc.count(&col_name, count_request, None, shard).await?;
                Ok(PointsResponse::Count(ret))
            }
            PointsRequest::Delete((col_name, selector)) => {
                let ret = do_delete_points(
                    toc,
                    &col_name,
                    selector,
                    None,
                    false,
                    WriteOrdering::default(),
                )
                .await?;
                Ok(PointsResponse::Delete(ret))
            }
            PointsRequest::Upsert((col_name, ops)) => {
                let ret =
                    do_upsert_points(toc, &col_name, ops, None, false, WriteOrdering::default())
                        .await?;
                Ok(PointsResponse::Upsert(ret))
            }
            PointsRequest::UpdateVectors((col_name, operations)) => {
                let ret = do_update_vectors(
                    toc,
                    &col_name,
                    operations,
                    None,
                    false,
                    WriteOrdering::default(),
                )
                .await?;
                Ok(PointsResponse::UpdateVectors(ret))
            }
            PointsRequest::DeleteVectors((col_name, operations)) => {
                let ret = do_delete_vectors(
                    toc,
                    &col_name,
                    operations,
                    None,
                    false,
                    WriteOrdering::default(),
                )
                .await?;
                Ok(PointsResponse::DeleteVectors(ret))
            }
            PointsRequest::SetPayload((col_name, payload)) => {
                let ret = do_set_payload(
                    toc,
                    &col_name,
                    payload,
                    None,
                    false,
                    WriteOrdering::default(),
                )
                .await?;
                Ok(PointsResponse::SetPayload(ret))
            }
            PointsRequest::OverwritePayload((col_name, payload)) => {
                let ret = do_overwrite_payload(
                    toc,
                    &col_name,
                    payload,
                    None,
                    false,
                    WriteOrdering::default(),
                )
                .await?;
                Ok(PointsResponse::OverwritePayload(ret))
            }
            PointsRequest::DeletePayload((col_name, payload)) => {
                let ret = do_delete_payload(
                    toc,
                    &col_name,
                    payload,
                    None,
                    false,
                    WriteOrdering::default(),
                )
                .await?;
                Ok(PointsResponse::DeletePayload(ret))
            }
            PointsRequest::ClearPayload((col_name, selector)) => {
                let ret = do_clear_payload(
                    toc,
                    &col_name,
                    selector,
                    None,
                    false,
                    WriteOrdering::default(),
                )
                .await?;
                Ok(PointsResponse::ClearPayload(ret))
            }
        }
    }
}

impl From<PointsRequest> for QdrantRequest {
    fn from(req: PointsRequest) -> Self {
        QdrantRequest::Points(req)
    }
}

async fn do_upsert_points(
    toc: &TableOfContent,
    collection_name: &str,
    operation: PointInsertOperations,
    shard_selection: Option<ShardId>,
    wait: bool,
    ordering: WriteOrdering,
) -> Result<UpdateResult, StorageError> {
    let (shard_key, operation) = operation.decompose();
    let collection_operation =
        CollectionUpdateOperations::PointOperation(PointOperations::UpsertPoints(operation));

    let shard_selector = get_shard_selector_for_update(shard_selection, shard_key);

    toc.update(
        collection_name,
        collection_operation,
        wait,
        ordering,
        shard_selector,
    )
    .await
}

async fn do_delete_points(
    toc: &TableOfContent,
    collection_name: &str,
    points: PointsSelector,
    shard_selection: Option<ShardId>,
    wait: bool,
    ordering: WriteOrdering,
) -> Result<UpdateResult, StorageError> {
    let (point_operation, shard_key) = match points {
        PointsSelector::PointIdsSelector(PointIdsList { points, shard_key }) => {
            (PointOperations::DeletePoints { ids: points }, shard_key)
        }
        PointsSelector::FilterSelector(FilterSelector { filter, shard_key }) => {
            (PointOperations::DeletePointsByFilter(filter), shard_key)
        }
    };
    let collection_operation = CollectionUpdateOperations::PointOperation(point_operation);
    let shard_selector = get_shard_selector_for_update(shard_selection, shard_key);

    toc.update(
        collection_name,
        collection_operation,
        wait,
        ordering,
        shard_selector,
    )
    .await
}

async fn do_update_vectors(
    toc: &TableOfContent,
    collection_name: &str,
    operation: UpdateVectors,
    shard_selection: Option<ShardId>,
    wait: bool,
    ordering: WriteOrdering,
) -> Result<UpdateResult, StorageError> {
    let UpdateVectors { points, shard_key } = operation;

    let collection_operation = CollectionUpdateOperations::VectorOperation(
        VectorOperations::UpdateVectors(UpdateVectorsOp { points }),
    );

    let shard_selector = get_shard_selector_for_update(shard_selection, shard_key);

    toc.update(
        collection_name,
        collection_operation,
        wait,
        ordering,
        shard_selector,
    )
    .await
}

async fn do_delete_vectors(
    toc: &TableOfContent,
    collection_name: &str,
    operation: DeleteVectors,
    shard_selection: Option<ShardId>,
    wait: bool,
    ordering: WriteOrdering,
) -> Result<UpdateResult, StorageError> {
    let DeleteVectors {
        vector,
        filter,
        points,
        shard_key,
    } = operation;

    let vector_names: Vec<_> = vector.into_iter().collect();

    let mut result = None;

    let shard_selector = get_shard_selector_for_update(shard_selection, shard_key);

    if let Some(filter) = filter {
        let vectors_operation =
            VectorOperations::DeleteVectorsByFilter(filter, vector_names.clone());
        let collection_operation = CollectionUpdateOperations::VectorOperation(vectors_operation);
        result = Some(
            toc.update(
                collection_name,
                collection_operation,
                wait,
                ordering,
                shard_selector.clone(),
            )
            .await?,
        );
    }

    if let Some(points) = points {
        let vectors_operation = VectorOperations::DeleteVectors(points.into(), vector_names);
        let collection_operation = CollectionUpdateOperations::VectorOperation(vectors_operation);
        result = Some(
            toc.update(
                collection_name,
                collection_operation,
                wait,
                ordering,
                shard_selector,
            )
            .await?,
        );
    }

    result.ok_or_else(|| StorageError::bad_request("No filter or points provided"))
}

async fn do_set_payload(
    toc: &TableOfContent,
    collection_name: &str,
    operation: SetPayload,
    shard_selection: Option<ShardId>,
    wait: bool,
    ordering: WriteOrdering,
) -> Result<UpdateResult, StorageError> {
    let SetPayload {
        points,
        payload,
        filter,
        shard_key,
    } = operation;

    let collection_operation =
        CollectionUpdateOperations::PayloadOperation(PayloadOps::SetPayload(SetPayloadOp {
            payload,
            points,
            filter,
        }));

    let shard_selector = get_shard_selector_for_update(shard_selection, shard_key);

    toc.update(
        collection_name,
        collection_operation,
        wait,
        ordering,
        shard_selector,
    )
    .await
}

async fn do_overwrite_payload(
    toc: &TableOfContent,
    collection_name: &str,
    operation: SetPayload,
    shard_selection: Option<ShardId>,
    wait: bool,
    ordering: WriteOrdering,
) -> Result<UpdateResult, StorageError> {
    let SetPayload {
        points,
        payload,
        filter,
        shard_key,
    } = operation;

    let collection_operation =
        CollectionUpdateOperations::PayloadOperation(PayloadOps::OverwritePayload(SetPayloadOp {
            payload,
            points,
            filter,
        }));

    let shard_selector = get_shard_selector_for_update(shard_selection, shard_key);

    toc.update(
        collection_name,
        collection_operation,
        wait,
        ordering,
        shard_selector,
    )
    .await
}

async fn do_delete_payload(
    toc: &TableOfContent,
    collection_name: &str,
    operation: DeletePayload,
    shard_selection: Option<ShardId>,
    wait: bool,
    ordering: WriteOrdering,
) -> Result<UpdateResult, StorageError> {
    let DeletePayload {
        keys,
        points,
        filter,
        shard_key,
    } = operation;

    let collection_operation =
        CollectionUpdateOperations::PayloadOperation(PayloadOps::DeletePayload(DeletePayloadOp {
            keys,
            points,
            filter,
        }));

    let shard_selector = get_shard_selector_for_update(shard_selection, shard_key);

    toc.update(
        collection_name,
        collection_operation,
        wait,
        ordering,
        shard_selector,
    )
    .await
}

async fn do_clear_payload(
    toc: &TableOfContent,
    collection_name: &str,
    points: PointsSelector,
    shard_selection: Option<ShardId>,
    wait: bool,
    ordering: WriteOrdering,
) -> Result<UpdateResult, StorageError> {
    let (point_operation, shard_key) = match points {
        PointsSelector::PointIdsSelector(PointIdsList { points, shard_key }) => {
            (PayloadOps::ClearPayload { points }, shard_key)
        }
        PointsSelector::FilterSelector(FilterSelector { filter, shard_key }) => {
            (PayloadOps::ClearPayloadByFilter(filter), shard_key)
        }
    };

    let collection_operation = CollectionUpdateOperations::PayloadOperation(point_operation);

    let shard_selector = get_shard_selector_for_update(shard_selection, shard_key);

    toc.update(
        collection_name,
        collection_operation,
        wait,
        ordering,
        shard_selector,
    )
    .await
}

/// Converts a pair of parameters into a shard selector
/// suitable for update operations.
///
/// The key difference from selector for search operations is that
/// empty shard selector in case of update means default shard,
/// while empty shard selector in case of search means all shards.
///
/// Parameters:
/// - shard_selection: selection of the exact shard ID, always have priority over shard_key
/// - shard_key: selection of the shard key, can be a single key or a list of keys
///
/// Returns:
/// - ShardSelectorInternal - resolved shard selector
fn get_shard_selector_for_update(
    shard_selection: Option<ShardId>,
    shard_key: Option<ShardKeySelector>,
) -> ShardSelectorInternal {
    match (shard_selection, shard_key) {
        (Some(shard_selection), None) => ShardSelectorInternal::ShardId(shard_selection),
        (Some(shard_selection), Some(_)) => {
            debug_assert!(
                false,
                "Shard selection and shard key are mutually exclusive"
            );
            ShardSelectorInternal::ShardId(shard_selection)
        }
        (None, Some(shard_key)) => ShardSelectorInternal::from(shard_key),
        (None, None) => ShardSelectorInternal::Empty,
    }
}

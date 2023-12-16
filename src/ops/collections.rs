use super::{shard_selector, ColName};
use crate::{Handler, QdrantRequest};
use async_trait::async_trait;
use collection::operations::{
    shard_key_selector::ShardKeySelector,
    types::{AliasDescription, CollectionInfo, CollectionsAliasesResponse},
};
use serde::{Deserialize, Serialize};
use storage::content_manager::{
    collection_meta_ops::{
        AliasOperations, ChangeAliasesOperation, CollectionMetaOperations, CreateAlias,
        CreateCollection, CreateCollectionOperation, DeleteAlias, DeleteCollectionOperation,
        RenameAlias, UpdateCollection, UpdateCollectionOperation,
    },
    errors::StorageError,
    toc::TableOfContent,
};

#[derive(Debug, Clone, Deserialize)]
pub enum CollectionRequest {
    /// list collections
    List,
    /// get collection with given name
    Get(ColName),
    /// create collection with given info
    Create((ColName, CreateCollection)),
    /// update collection with given info
    Update((ColName, UpdateCollection)),
    /// delete collection with given name
    Delete(ColName),
    // CreateIndex((ColName, CreateFieldIndex)),
}

#[derive(Debug, Clone, Deserialize)]
pub enum AliasRequest {
    /// list aliases
    List,
    /// get aliases for a given collection
    Get(ColName),
    /// create alias with given collection name and alias name
    Create((ColName, String)),
    /// delete alias with alias name
    Delete(String),
    /// rename alias with old and new alias names
    Rename((String, String)),
}

#[derive(Debug, Serialize)]
pub enum CollectionResponse {
    /// list collections
    List(Vec<String>),
    /// collection info
    Get(CollectionInfo),
    /// creation status
    Create(bool),
    /// update status
    Update(bool),
    /// deletion status
    Delete(bool),
    // CreateIndex(String),
}

#[derive(Debug, Serialize)]
pub enum AliasResponse {
    /// list aliases
    List(CollectionsAliasesResponse),
    /// alias info
    Get(CollectionsAliasesResponse),
    /// creation status
    Create(bool),
    /// deletion status
    Delete(bool),
    /// rename status
    Rename(bool),
}

#[async_trait]
impl Handler for CollectionRequest {
    type Response = CollectionResponse;
    type Error = StorageError;

    async fn handle(self, toc: &TableOfContent) -> Result<Self::Response, Self::Error> {
        match self {
            CollectionRequest::List => {
                let collections = toc.all_collections().await;
                Ok(CollectionResponse::List(collections))
            }
            CollectionRequest::Get(name) => {
                let collection = do_get_collection(toc, &name, None).await?;
                Ok(CollectionResponse::Get(collection))
            }

            CollectionRequest::Create((name, op)) => {
                let op = CollectionMetaOperations::CreateCollection(
                    CreateCollectionOperation::new(name, op),
                );
                toc.check_write_lock()?;
                let ret = toc.perform_collection_meta_op(op).await?;

                Ok(CollectionResponse::Create(ret))
            }
            CollectionRequest::Update((name, op)) => {
                let op = CollectionMetaOperations::UpdateCollection(
                    UpdateCollectionOperation::new(name, op),
                );
                let ret = toc.perform_collection_meta_op(op).await?;

                Ok(CollectionResponse::Update(ret))
            }
            CollectionRequest::Delete(name) => {
                let op =
                    CollectionMetaOperations::DeleteCollection(DeleteCollectionOperation(name));
                let ret = toc.perform_collection_meta_op(op).await?;

                Ok(CollectionResponse::Delete(ret))
            }
        }
    }
}

#[async_trait]
impl Handler for AliasRequest {
    type Response = AliasResponse;
    type Error = StorageError;

    async fn handle(self, toc: &TableOfContent) -> Result<Self::Response, Self::Error> {
        match self {
            AliasRequest::List => {
                let aliases = do_list_aliases(toc).await?;
                Ok(AliasResponse::List(aliases))
            }
            AliasRequest::Get(name) => {
                let aliases = do_list_collection_aliases(toc, &name).await?;
                Ok(AliasResponse::Get(aliases))
            }
            AliasRequest::Create((collection_name, alias_name)) => {
                let op = create_alias_op(collection_name, alias_name);
                let op = CollectionMetaOperations::ChangeAliases(op);

                let ret = toc.perform_collection_meta_op(op).await?;

                Ok(AliasResponse::Create(ret))
            }
            AliasRequest::Delete(name) => {
                let op = delete_alias_op(name);
                let op = CollectionMetaOperations::ChangeAliases(op);

                let ret = toc.perform_collection_meta_op(op).await?;

                Ok(AliasResponse::Delete(ret))
            }
            AliasRequest::Rename((old_name, new_name)) => {
                let op = rename_alias_op(old_name, new_name);
                let op = CollectionMetaOperations::ChangeAliases(op);

                let ret = toc.perform_collection_meta_op(op).await?;

                Ok(AliasResponse::Rename(ret))
            }
        }
    }
}

impl From<CollectionRequest> for QdrantRequest {
    fn from(req: CollectionRequest) -> Self {
        QdrantRequest::Collection(req)
    }
}

impl From<AliasRequest> for QdrantRequest {
    fn from(req: AliasRequest) -> Self {
        QdrantRequest::Alias(req)
    }
}

fn create_alias_op(collection_name: String, alias_name: String) -> ChangeAliasesOperation {
    let op = CreateAlias {
        collection_name,
        alias_name,
    };
    let op = AliasOperations::from(op);
    ChangeAliasesOperation { actions: vec![op] }
}

fn delete_alias_op(alias_name: String) -> ChangeAliasesOperation {
    let op = DeleteAlias { alias_name };
    let op = AliasOperations::from(op);
    ChangeAliasesOperation { actions: vec![op] }
}

fn rename_alias_op(old_alias_name: String, new_alias_name: String) -> ChangeAliasesOperation {
    let op = RenameAlias {
        old_alias_name,
        new_alias_name,
    };
    let op = AliasOperations::from(op);
    ChangeAliasesOperation { actions: vec![op] }
}

async fn do_list_aliases(toc: &TableOfContent) -> Result<CollectionsAliasesResponse, StorageError> {
    let aliases = toc.list_aliases().await?;
    Ok(CollectionsAliasesResponse { aliases })
}

async fn do_list_collection_aliases(
    toc: &TableOfContent,
    collection_name: &str,
) -> Result<CollectionsAliasesResponse, StorageError> {
    let mut aliases: Vec<AliasDescription> = Default::default();
    for alias in toc.collection_aliases(collection_name).await? {
        aliases.push(AliasDescription {
            alias_name: alias.to_string(),
            collection_name: collection_name.to_string(),
        });
    }
    Ok(CollectionsAliasesResponse { aliases })
}

async fn do_get_collection(
    toc: &TableOfContent,
    name: &str,
    shard_key: Option<ShardKeySelector>,
) -> Result<CollectionInfo, StorageError> {
    let collection = toc.get_collection(name).await?;

    let shard = shard_selector(shard_key);

    Ok(collection.info(&shard).await?)
}

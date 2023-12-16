mod collections;
mod points;
mod query;

use collection::operations::{
    shard_key_selector::ShardKeySelector, shard_selector_internal::ShardSelectorInternal,
};

pub use collections::*;
pub use points::*;
pub use query::*;

pub type ColName = String;

fn shard_selector(shard_key: Option<ShardKeySelector>) -> ShardSelectorInternal {
    match shard_key {
        None => ShardSelectorInternal::All,
        Some(shard_keys) => shard_keys.into(),
    }
}

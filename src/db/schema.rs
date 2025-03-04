use std::collections::HashMap;
use indexmap::IndexMap;
use crate::db::data::ColumnType;

#[derive(Debug, Clone)]
pub struct Keyspace {
    pub name: String,
    pub tables: HashMap<String, TableMetadata>,
}

#[derive(Debug, Clone)]
pub enum Key {
    Single(String),
    Composite(Vec<String>)
}

impl Key {
    fn to_bytes(&self) -> Vec<u8> {
        match self {
            Key::Single(key) => key.as_bytes().to_vec(),
            Key::Composite(keys) => keys.iter().flat_map(|key| key.as_bytes().to_vec()).collect()
        }
    }
}

#[derive(Debug, Clone)]
pub struct TableMetadata {
    pub keyspace: String,
    pub name: String,
    pub partition_key: Key,
    pub cluster_key: Option<Key>,
    pub columns: IndexMap<String, ColumnMetadata>
}

#[derive(Debug, Clone)]
pub struct ColumnMetadata {
    pub name: String,
    pub column_type: ColumnType,
    pub kind: Kind
}

#[derive(Debug, Clone)]
pub enum Kind {
    PartitionKey,
    Clustering,
    Regular,
    Static
}
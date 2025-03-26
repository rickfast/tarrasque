use crate::db::data::ColumnType;
use indexmap::IndexMap;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct Keyspace {
    pub name: String,
    pub tables: HashMap<String, TableMetadata>,
}

#[derive(Debug, Clone)]
pub struct TableMetadata {
    pub keyspace: String,
    pub name: String,
    pub partition_key: Vec<String>,
    pub cluster_key: Vec<String>,
    pub columns: IndexMap<String, ColumnMetadata>,
}

impl TableMetadata {
    pub fn ordered_column_names(&self) -> Vec<String> {
        self.columns
            .keys()
            .map(|name| name.to_string())
            .collect::<Vec<String>>()
    }
}

#[derive(Debug, Clone)]
pub struct ColumnMetadata {
    pub name: String,
    pub column_type: ColumnType,
    pub kind: Kind,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Kind {
    PartitionKey,
    Clustering,
    Regular,
    Static,
}

use crate::db::data::ColumnType;
use indexmap::IndexMap;
use std::collections::HashMap;

pub type Tables = HashMap<String, TableMetadata>;

#[derive(Debug)]
pub struct Keyspace<'a> {
    pub name: String,
    pub tables: &'a mut Tables,
}

impl Keyspace<'_> {
    pub fn create_table(&mut self, table: TableMetadata) {
        self.tables.insert(table.name.clone(), table);
    }
}

#[derive(Debug, Clone)]
pub struct TableMetadata {
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

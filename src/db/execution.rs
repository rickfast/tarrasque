use crate::db::builtins::Function;
use crate::db::data::{Row, Value};
use crate::db::error::{DbError, ErrorCode};
use crate::db::parse::{ParsedExpr, ParsedInsert, ParsedQuery};
use crate::db::schema::{TableMetadata, Tables};
use fjall::{Keyspace, KvPair, PartitionCreateOptions};
use std::collections::HashMap;
use std::iter::empty;
use std::ops::Not;
use std::sync::Arc;
use tokio::sync::RwLock;

pub fn execute_insert<'a>(
    keyspace: &Keyspace,
    parsed_insert: ParsedInsert,
) -> Result<impl Iterator<Item = Vec<Option<Value>>>, DbError> {
    let table = &parsed_insert.table;
    let partition = keyspace
        .open_partition(&table.name, PartitionCreateOptions::default())
        .map_err(|err| DbError::new(ErrorCode::WriteFailure, err.to_string()))?;
    let prefix = [
        &parsed_insert.partition_key[..],
        &parsed_insert.clustering_key[..],
    ]
    .concat()
    .join("");

    println!("[INSERT] prefix: {}", prefix);

    let row = Row {
        columns: parsed_insert
            .values
            .iter()
            .map(|value| value.resolve(HashMap::new(), &HashMap::new()))
            .collect(),
    };

    partition
        .insert(prefix, row)
        .map_err(|err| DbError::new(ErrorCode::WriteFailure, err.to_string()))?;

    Ok(empty())
}

pub fn execute_select<'a>(
    keyspace: &Keyspace,
    parsed_query: ParsedQuery,
) -> Result<impl Iterator<Item = Vec<Option<Value>>>, DbError> {
    let table = &parsed_query.table;
    let partition = keyspace
        .open_partition(&table.name, PartitionCreateOptions::default())
        .map_err(|err| DbError::new(ErrorCode::ReadFailure, err.to_string()))?;
    let prefix = [
        &parsed_query.partition_key[..],
        &parsed_query.clustering_key[..],
    ]
    .concat()
    .join("");

    fn unwrap_values(row: &HashMap<String, Option<Value>>) -> HashMap<String, Value> {
        row.iter()
            .filter(|(_, v)| v.is_some())
            .map(|(k, v)| (k.clone(), v.clone().unwrap()))
            .collect()
    }

    let iterator: Box<dyn DoubleEndedIterator<Item = fjall::Result<KvPair>>> =
        if prefix.is_empty().not() {
            Box::new(partition.prefix(prefix))
        } else {
            Box::new(partition.iter())
        };
    let ordered_columns = table.ordered_column_names();
    let results = iterator
        .map(|raw_row| Row::from(raw_row.unwrap().1))
        .map(move |row| {
            let mut columns: HashMap<String, Option<Value>> = HashMap::new();

            for n in 0..row.columns.len() {
                let column_name = ordered_columns[n].clone();
                columns.insert(column_name, row.columns[n].clone());
            }

            columns
        })
        .map(move |row| {
            parsed_query
                .projection
                .iter()
                .map(|expr| expr.resolve(unwrap_values(&row), &HashMap::new()))
                .collect::<Vec<_>>()
        });

    Ok(results)
}

pub async fn execute_create_table(
    table_metadata: &TableMetadata,
    tables: &Arc<RwLock<Tables>>,
) -> Result<impl Iterator<Item = Vec<Option<Value>>>, DbError> {
    tables
        .write()
        .await
        .insert(table_metadata.name.clone(), table_metadata.clone());

    Ok(empty())
}

impl ParsedExpr {
    fn resolve(
        &self,
        row: HashMap<String, Value>,
        catalog: &HashMap<String, Function>,
    ) -> Option<Value> {
        match self {
            ParsedExpr::Column(column) => {
                let column_name = &column.target_column;

                row.get(column_name).map(|value| value.to_owned())
            }
            ParsedExpr::Function(function_handle, parameters) => {
                let function = catalog.get(function_handle).unwrap();
                let values: Vec<Option<Value>> = parameters
                    .iter()
                    .map(|expr| expr.resolve(row.clone(), &catalog))
                    .collect();

                Some(function(values))
            }
            ParsedExpr::Literal(value) => value.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::data::{row, ColumnType, Row, Value};
    use crate::db::parse::ProjectedColumn;
    use crate::db::schema::{ColumnMetadata, Keyspace, Kind, TableMetadata, Tables};
    use fjall::Keyspace as FjallKeyspace;
    use fjall::{Config, PartitionCreateOptions};
    use indexmap::IndexMap;

    use crate::db::Database;

    use tokio::sync::RwLock;

    #[tokio::test]
    async fn test_execute_full_flow() {
        // Step 1: Create a mock table metadata
        let mut columns = IndexMap::new();
        columns.insert(
            "id".to_string(),
            ColumnMetadata {
                name: "id".to_string(),
                column_type: ColumnType::Int,
                kind: Kind::PartitionKey,
            },
        );
        columns.insert(
            "name".to_string(),
            ColumnMetadata {
                name: "name".to_string(),
                column_type: ColumnType::Varchar,
                kind: Kind::Regular,
            },
        );

        let table_metadata = TableMetadata {
            name: "users".to_string(),
            columns,
            partition_key: vec!["id".to_string()],
            cluster_key: vec![],
        };

        let tables = Arc::new(RwLock::new(Tables::new()));
        let fjall = FjallKeyspace::open(Config::new("/tmp/x")).unwrap();

        // Step 2: Execute create table
        let create_result = execute_create_table(&table_metadata, &tables).await;

        assert!(create_result.is_ok());
        assert!(tables.read().await.contains_key("users"));

        // Step 3: Prepare and execute an insert
        let parsed_insert = ParsedInsert {
            table: table_metadata.clone(),
            partition_key: vec!["1".to_string()],
            clustering_key: vec![],
            values: vec![
                ParsedExpr::Literal(Some(Value::Int(1))),
                ParsedExpr::Literal(Some(Value::Varchar("John Doe".to_string()))),
            ],
        };

        let insert_result = execute_insert(&fjall, parsed_insert);
        assert!(insert_result.is_ok());

        // Step 4: Prepare and execute a select
        let parsed_query = ParsedQuery {
            table: table_metadata.clone(),
            partition_key: vec!["1".to_string()],
            clustering_key: vec![],
            projection: vec![
                ParsedExpr::Column(ProjectedColumn {
                    target_column: "id".to_string(),
                    resolved_name: "id".to_string(),
                    column_metadata: table_metadata.columns.get("id").unwrap().clone(),
                }),
                ParsedExpr::Column(ProjectedColumn {
                    target_column: "name".to_string(),
                    resolved_name: "name".to_string(),
                    column_metadata: table_metadata.columns.get("name").unwrap().clone(),
                }),
            ],
            filters: HashMap::new(),
            column_count: 2,
        };

        let select_result = execute_select(&fjall, parsed_query);
        assert!(select_result.is_ok());

        let mut result_iter = select_result.unwrap();
        let row = result_iter.next().unwrap();

        // Step 5: Verify the selected data
        assert_eq!(row.len(), 2);
        assert_eq!(row[0], Some(Value::Int(1)));
        assert_eq!(row[1], Some(Value::Varchar("John Doe".to_string())));
    }

    #[test]
    fn test_execute() {
        // Create a mock keyspace with a table and columns
        let mut columns = IndexMap::new();

        columns.insert(
            "id".to_string(),
            ColumnMetadata {
                name: "id".to_string(),
                column_type: ColumnType::Smallint,
                kind: Kind::PartitionKey,
            },
        );
        columns.insert(
            "name".to_string(),
            ColumnMetadata {
                name: "name".to_string(),
                column_type: ColumnType::Varchar,
                kind: Kind::Regular,
            },
        );

        let table = TableMetadata {
            name: "users".to_string(),
            columns,
            partition_key: vec!["id".to_string()],
            cluster_key: vec![],
        };
        let mut tables = &mut Tables::new();

        tables.insert("users".to_string(), table.clone());

        let keyspace = Keyspace {
            name: "test_keyspace".to_string(),
            tables: &mut tables,
        };

        let ks = FjallKeyspace::open(Config::new("/tmp/x")).unwrap();
        let row_a = row![Value::Smallint(1), Value::Varchar("row1".to_string())];
        let row_b = row![Value::Smallint(2), Value::Varchar("row2".to_string())];
        let partition = ks
            .open_partition(&table.name, PartitionCreateOptions::default())
            .unwrap();

        partition.insert("1", row_a).unwrap();
        partition.insert("2", row_b).unwrap();

        // Define a simple ParsedQuery
        let parsed_query = ParsedQuery {
            partition_key: vec![1u16.to_string()],
            clustering_key: vec![],
            projection: vec![
                ParsedExpr::Column(ProjectedColumn {
                    target_column: "id".to_string(),
                    resolved_name: "id".to_string(),
                    column_metadata: table.columns.get("id").unwrap().clone(),
                }),
                ParsedExpr::Column(ProjectedColumn {
                    target_column: "name".to_string(),
                    resolved_name: "name".to_string(),
                    column_metadata: table.columns.get("name").unwrap().clone(),
                }),
            ],
            filters: HashMap::new(),
            table,
            column_count: 2,
        };

        // Call the execute function
        let result = execute_select(&ks, parsed_query);

        // Check the result
        assert!(result.is_ok());

        let mut result_iter = result.unwrap();
        let row = result_iter.next().unwrap();

        assert_eq!(row.len(), 2);
        assert_eq!(row[0], Some(Value::Smallint(1)));
        assert_eq!(row[1], Some(Value::Varchar("row1".to_string())));
    }

    #[test]
    fn test_execute_create_table() {
        tokio_test::block_on(async {
            // Create a mock table metadata
            let mut columns = IndexMap::new();
            columns.insert(
                "id".to_string(),
                ColumnMetadata {
                    name: "id".to_string(),
                    column_type: ColumnType::Int,
                    kind: Kind::PartitionKey,
                },
            );
            columns.insert(
                "name".to_string(),
                ColumnMetadata {
                    name: "name".to_string(),
                    column_type: ColumnType::Varchar,
                    kind: Kind::Regular,
                },
            );

            let table_metadata = TableMetadata {
                name: "users".to_string(),
                columns,
                partition_key: vec!["id".to_string()],
                cluster_key: vec![],
            };

            let tables = Tables::new();
            let binding = Arc::new(RwLock::new(tables));
            let fjall = FjallKeyspace::open(Config::new("/tmp/x")).unwrap();
            let database = Arc::new(RwLock::new(Database {
                name: "test_db",
                tables: &binding,
                fjall: &fjall,
            }));

            let result =
                execute_create_table(&table_metadata, Arc::clone(&database).read().await.tables)
                    .await;

            assert!(result.is_ok());

            let db = Arc::clone(&database);

            assert!(db.read().await.tables.read().await.contains_key("users"));

            let db2 = db.read().await;
            let tables2 = db2.tables.read().await;
            let created_table = tables2.get("users").unwrap();

            assert_eq!(created_table.name, "users");
            assert_eq!(created_table.columns.len(), 2);
            assert_eq!(created_table.columns.get("id").unwrap().name, "id");
            assert_eq!(created_table.columns.get("name").unwrap().name, "name");
        })
    }
}

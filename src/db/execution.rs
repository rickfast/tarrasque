use crate::db::data::{Row, Value};
use crate::db::parse::{ParsedExpr, ParsedQuery};
use fjall::{Keyspace, KvPair, PartitionCreateOptions};
use std::collections::HashMap;
fn execute<'a>(
    keyspace: &Keyspace,
    parsed_query: ParsedQuery,
) -> anyhow::Result<impl Iterator<Item = Vec<Value>>> {
    let table = &parsed_query.table;
    let partition = keyspace.open_partition(&table.name, PartitionCreateOptions::default())?;
    // let prefix: Vec<String> = [
    //     // &parsed_query.partition_key[..],
    //     // &parsed_query.clustering_key[..],
    // ]
    // .concat();
    let iterator: Box<dyn DoubleEndedIterator<Item = fjall::Result<KvPair>>> =
        // if prefix.is_empty().not() {
        //     let prefix_bytes = prefix
        //         .iter()
        //         .flat_map(|item| item.clone().into_bytes())
        //         .collect::<Vec<u8>>();
        //     Box::new(partition.prefix(prefix_bytes))
        // } else {
            Box::new(partition.iter());
    // };
    let ordered_columns = table.ordered_column_names();
    let results = iterator
        .map(|raw_row| Row::from(raw_row.unwrap().1))
        .map(move |row| {
            let mut columns: HashMap<String, Value> = HashMap::new();

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
                .map(|expr| expr.resolve(row.clone(), &HashMap::new()))
                .collect::<Vec<_>>()
        });

    Ok(results)
}

type Function = fn(Vec<Value>) -> Value;

impl ParsedExpr {
    fn resolve(&self, row: HashMap<String, Value>, catalog: &HashMap<String, Function>) -> Value {
        match self {
            ParsedExpr::Column(column) => {
                let column_name = &column.target_column;

                row.get(column_name).unwrap().clone()
            }
            ParsedExpr::Function(function_handle, parameters) => {
                let function = catalog.get(function_handle).unwrap();
                let values = parameters
                    .iter()
                    .map(|expr| expr.resolve(row.clone(), &catalog))
                    .collect();

                function(values)
            }
            ParsedExpr::Literal(value) => value.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::data::{ColumnType, Row, Value};
    use crate::db::parse::ProjectedColumn;
    use crate::db::schema::{ColumnMetadata, Keyspace, Kind, TableMetadata};
    use fjall::Keyspace as FjallKeyspace;
    use fjall::{Config, PartitionCreateOptions, Slice};
    use indexmap::IndexMap;
    use std::collections::HashMap;

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
            keyspace: "test_keyspace".to_string(),
            partition_key: vec!["id".to_string()],
            cluster_key: vec![],
        };
        let mut tables = HashMap::new();

        tables.insert("users".to_string(), table.clone());

        let keyspace = Keyspace {
            name: "test_keyspace".to_string(),
            tables,
        };

        let ks = FjallKeyspace::open(Config::new("/tmp/x")).unwrap();

        let row_a = Row {
            columns: vec![Value::Smallint(1), Value::Varchar("row1".to_string())],
        };
        let row_b = Row {
            columns: vec![Value::Smallint(2), Value::Varchar("row2".to_string())],
        };

        // Create a mock partition with data
        let partition = ks
            .open_partition(&table.name, PartitionCreateOptions::default())
            .unwrap();

        partition
            .insert(Slice::from(1u16.to_be_bytes()), row_a)
            .unwrap();
        partition
            .insert(Slice::from(2u16.to_be_bytes()), row_b)
            .unwrap();

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
            filters: vec![],
            table,
        };

        // Call the execute function
        let result = execute(&ks, parsed_query);

        // Check the result
        assert!(result.is_ok());
        let mut result_iter = result.unwrap();

        // Check the first row
        let row = result_iter.next().unwrap();
        assert_eq!(row.len(), 2);
        assert_eq!(row[0], Value::Smallint(1));
        assert_eq!(row[1], Value::Varchar("row1".to_string()));

        // Check the second row
        let row = result_iter.next().unwrap();
        assert_eq!(row.len(), 2);
        assert_eq!(row[0], Value::Smallint(2));
        assert_eq!(row[1], Value::Varchar("row2".to_string()));
    }
}

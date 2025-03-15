use crate::db::data::Value;
use crate::db::dialect::CassandraDialect;
use crate::db::schema::{ColumnMetadata, Keyspace, TableMetadata};
use anyhow::anyhow;
use sqlparser::ast::{BinaryOperator, Expr, Select, SelectItem, SetExpr, Statement, TableFactor};
use sqlparser::parser::Parser;
use std::ops::Deref;

#[derive(Debug, Clone)]
pub struct ParsedQuery {
    pub partition_key: Vec<String>,
    pub clustering_key: Vec<String>,
    pub projection: Vec<ParsedExpr>,
    pub filters: Vec<ParsedExpr>,
    pub table: TableMetadata,
}

#[derive(Debug, Clone)]
pub enum ParsedExpr {
    Column(ProjectedColumn),
    Function(FunctionHandle, Vec<ParsedExpr>),
    Literal(Value),
}

pub type FunctionHandle = String;

#[derive(Debug, Clone)]
pub struct ProjectedColumn {
    pub target_column: String,
    pub resolved_name: String,
    pub column_metadata: ColumnMetadata,
}

// Parse SQL query
fn parse<'a>(sql: String, keyspace: Keyspace) -> anyhow::Result<ParsedQuery> {
    let dialect = CassandraDialect {};
    let statements = Parser::parse_sql(&dialect, &sql)?;

    if statements.len() != 1 {
        return Err(anyhow::anyhow!("Only one statement is supported"));
    }

    let statement = &statements[0];

    if let Statement::Query(query) = statement {
        if let SetExpr::Select(select) = &query.body.deref() {
            let table = derive_table_metadata(&keyspace, &select)?;
            let projection = derive_projection(&select, &table)?;

            Ok(ParsedQuery {
                filters: vec![],
                partition_key: vec![],
                clustering_key: vec![],
                projection,
                table: table.clone(),
            })
        } else {
            Err(anyhow!(""))
        }
    } else {
        Err(anyhow::anyhow!("Only SELECT statements are supported"))
    }
}

fn derive_projection(
    select: &Box<Select>,
    table: &TableMetadata,
) -> anyhow::Result<Vec<ParsedExpr>> {
    select
        .projection
        .iter()
        .map(|item| match item {
            SelectItem::UnnamedExpr(expr) => match expr {
                Expr::Identifier(ident) => {
                    let column_name = ident.value.clone();
                    let column_metadata = table.columns.get(&column_name);

                    match column_metadata {
                        Some(metadata) => Ok(ParsedExpr::Column(ProjectedColumn {
                            target_column: column_name.clone(),
                            resolved_name: column_name.clone(),
                            column_metadata: metadata.clone(),
                        })),
                        None => Err(anyhow!("Error")),
                    }
                }
                _ => unimplemented!(),
            },
            SelectItem::ExprWithAlias { expr, alias } => match expr {
                Expr::Identifier(ident) => {
                    let column_name = ident.value.clone();
                    let column_metadata = table.columns.get(&column_name);
                    match column_metadata {
                        Some(metadata) => Ok(ParsedExpr::Column(ProjectedColumn {
                            target_column: column_name.clone(),
                            resolved_name: alias.value.clone(),
                            column_metadata: metadata.clone(),
                        })),
                        None => Err(anyhow!("Error")),
                    }
                }
                _ => unimplemented!(),
            },
            _ => unimplemented!(),
        })
        .collect::<anyhow::Result<Vec<ParsedExpr>>>()
}

fn derive_table_metadata<'a>(
    keyspace: &'a Keyspace,
    select: &Box<Select>,
) -> anyhow::Result<&'a TableMetadata> {
    let table = match &select.from.first().unwrap().relation {
        TableFactor::Table { name, .. } => {
            let table_name = name.to_string();
            keyspace
                .tables
                .get(&table_name)
                .ok_or_else(|| anyhow::anyhow!("Table not found"))?
        }
        _ => unimplemented!(),
    };
    Ok(table)
}

// Function to check if the WHERE clause uses a partition key
fn analyze_where_clause(
    expr: &Expr,
    partition_key: &str,
    clustering_key: Option<&str>,
) -> (bool, bool) {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            if let Expr::Identifier(ident) = &**left {
                if ident.value == partition_key {
                    if matches!(op, BinaryOperator::Eq) {
                        return (true, false); // Partition key is fully specified
                    }
                } else if let Some(cluster_key) = clustering_key {
                    if ident.value == cluster_key {
                        return (false, true); // Clustering key is used
                    }
                }
            }
        }
        _ => {}
    }
    (false, false) // No partition or clustering key match
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::data::ColumnType;
    use crate::db::schema::{ColumnMetadata, Keyspace, Kind, TableMetadata};
    use indexmap::IndexMap;
    use std::collections::HashMap;

    #[test]
    fn test_parse() {
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

        tables.insert("users".to_string(), table);

        let keyspace = Keyspace {
            name: "test_keyspace".to_string(),
            tables,
        };

        // Define a simple SQL query
        let sql = "SELECT id, name FROM users".to_string();

        // Call the parse function
        let result = parse(sql, keyspace);

        // Check the result
        assert!(result.is_ok());
        let parsed_query = result.unwrap();
        assert_eq!(parsed_query.table.name, "users");
        assert_eq!(parsed_query.projection.len(), 2);

        if let ParsedExpr::Column(column) = &parsed_query.projection[0] {
            assert_eq!(column.resolved_name, "id");
            assert_eq!(column.column_metadata.name, "id");
        } else {
            panic!("Expected ParsedExpr::Column");
        }

        if let ParsedExpr::Column(column) = &parsed_query.projection[1] {
            assert_eq!(column.resolved_name, "name");
            assert_eq!(column.column_metadata.name, "name");
        } else {
            panic!("Expected ParsedExpr::Column");
        }

        //assert_eq!(parsed_query.projection[0].resolved_name, "id");
        //assert_eq!(parsed_query.projection[1].resolved_name, "name");
    }
}

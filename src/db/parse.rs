use crate::db::builtins::{eq, neq, FilterFunction};
use crate::db::data::{ColumnType, Value};
use crate::db::dialect::CassandraDialect;
use crate::db::error::{DbError, ErrorCode};
use crate::db::schema::{ColumnMetadata, Kind, TableMetadata, Tables};
use anyhow::anyhow;
use indexmap::IndexMap;
use sqlparser::ast::{
    BinaryOperator, ColumnOption, CreateTable, Expr, Query, Select, SelectItem, SetExpr, Statement,
    TableFactor,
};
use sqlparser::parser::Parser;
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;
use tokio::sync::RwLock;

pub enum ParsedStatement {
    Select(ParsedQuery),
    Create(TableMetadata),
    Insert(ParsedInsert),
}

#[derive(Debug, Clone)]
pub struct ParsedQuery {
    pub partition_key: Vec<String>,
    pub clustering_key: Vec<String>,
    pub projection: Vec<ParsedExpr>,
    pub filters: HashMap<String, ParsedFilter>,
    pub table: TableMetadata,
    pub column_count: i32,
}

#[derive(Debug, Clone)]
pub struct ParsedFilter {
    pub filter: FilterFunction,
    pub args: Vec<ParsedExpr>,
}

#[derive(Debug, Clone)]
pub struct ParsedInsert {
    pub table: TableMetadata,
    pub partition_key: Vec<String>,
    pub clustering_key: Vec<String>,
    pub values: Vec<ParsedExpr>,
}

#[derive(Debug, Clone)]
pub enum ParsedExpr {
    Column(ProjectedColumn),
    Function(FunctionHandle, Vec<ParsedExpr>),
    Literal(Option<Value>),
}

pub type FunctionHandle = String;

#[derive(Debug, Clone)]
pub struct ProjectedColumn {
    pub target_column: String,
    pub resolved_name: String,
    pub column_metadata: ColumnMetadata,
}

// Parse SQL query
pub async fn parse<'a>(
    sql: String,
    table_metadata: &Arc<RwLock<Tables>>,
) -> Result<ParsedStatement, DbError> {
    let dialect = CassandraDialect {};
    let statements = Parser::parse_sql(&dialect, &sql)
        .map_err(|error| DbError::new(ErrorCode::Invalid, error.to_string()))?;

    if statements.len() != 1 {
        return Err(DbError::new(
            ErrorCode::Invalid,
            "Only one statement is supported".to_string(),
        ));
    }

    let statement = &statements[0];

    match statement {
        Statement::CreateTable(create_table) => parse_create_table(&create_table),
        Statement::Query(query) => parse_select(table_metadata, &query).await,
        Statement::Insert(insert) => parse_insert(table_metadata, &insert).await,
        _ => {
            unimplemented!()
        }
    }
}

// Parse SELECT statement
async fn parse_select(
    table_metadata: &Arc<RwLock<Tables>>,
    query: &Box<Query>,
) -> Result<ParsedStatement, DbError> {
    if let SetExpr::Select(select) = &query.body.deref() {
        let table = derive_table_metadata(&table_metadata, &select)
            .await
            .map_err(|error| DbError::new(ErrorCode::Invalid, "".to_string()))?;
        let projection: Vec<ParsedExpr> = derive_projection(&select, &table)
            .map_err(|error| DbError::new(ErrorCode::Invalid, "".to_string()))?;
        let (filters, partition_key) = derive_filters(&select, &table)
            .map_err(|error| DbError::new(ErrorCode::Invalid, "".to_string()))?;

        Ok(ParsedStatement::Select(ParsedQuery {
            filters,
            partition_key,
            clustering_key: vec![],
            projection: projection.clone(),
            table: table.clone(),
            column_count: projection.len() as i32,
        }))
    } else {
        unimplemented!()
    }
}

async fn parse_insert(
    table_metadata: &Arc<RwLock<Tables>>,
    insert: &sqlparser::ast::Insert,
) -> Result<ParsedStatement, DbError> {
    let table_name = insert.table_name.to_string();
    let table = table_metadata
        .read()
        .await
        .get(&table_name)
        .ok_or_else(|| DbError::new(ErrorCode::Invalid, "Table not found".to_string()))?
        .clone();

    let mut partition_key = vec![];
    let mut clustering_key = vec![];
    let mut values = vec![];

    for (i, column) in insert.columns.iter().enumerate() {
        let column_name = column.value.clone();

        let column_metadata = table.columns.get(&column_name).ok_or_else(|| {
            DbError::new(
                ErrorCode::Invalid,
                format!("Column {} not found", column_name),
            )
        })?;

        // let value_expr = &insert.source.clone().unwrap().body.as_ref().unwrap().values().unwrap()[0][i];
        let body = *insert.source.clone().unwrap().body;
        let value_expr = match body {
            SetExpr::Values(ref values) => {
                if i < values.rows[0].len() {
                    &values.rows[0][i]
                } else {
                    return Err(DbError::new(
                        ErrorCode::Invalid,
                        "Value not found".to_string(),
                    ));
                }
            }
            _ => {
                return Err(DbError::new(
                    ErrorCode::Invalid,
                    "Unsupported insert source".to_string(),
                ))
            }
        };
        let value = match value_expr {
            Expr::Value(val) => {
                if column_metadata.kind == Kind::PartitionKey {
                    partition_key.push(format!("{}", val));
                } else if column_metadata.kind == Kind::Clustering {
                    clustering_key.push(format!("{}", val));
                }
                ParsedExpr::Literal(Value::from_sql_value(column_metadata.column_type, val))
            }
            _ => {
                return Err(DbError::new(
                    ErrorCode::Invalid,
                    "Unsupported value expression".to_string(),
                ))
            }
        };

        values.push(value);

        match column_metadata.kind {
            Kind::PartitionKey => partition_key.push(column_name),
            Kind::Clustering => clustering_key.push(column_name),
            _ => {}
        }
    }

    Ok(ParsedStatement::Insert(ParsedInsert {
        table,
        partition_key,
        clustering_key,
        values,
    }))
}

fn parse_create_table(create_table: &CreateTable) -> Result<ParsedStatement, DbError> {
    let mut columns = IndexMap::new();

    for column_def in &create_table.columns {
        let column_name = column_def.name.value.clone();
        let partition = column_def
            .options
            .iter()
            .any(move |option| match option.option {
                ColumnOption::Unique { is_primary, .. } => is_primary,
                _ => false,
            });

        let kind = if partition {
            Kind::PartitionKey
        } else {
            Kind::Regular
        };

        let column_type = column_def.data_type.to_string().to_lowercase();

        columns.insert(
            column_name.clone(),
            ColumnMetadata {
                name: column_name,
                column_type: ColumnType::from_cql_type(column_type).unwrap(),
                kind,
            },
        );
    }

    let table_name = create_table.name.to_string();
    let partition_keys = columns
        .iter()
        .filter(|(_, col)| col.kind == Kind::PartitionKey)
        .map(|(name, _)| name.clone())
        .collect();

    Ok(ParsedStatement::Create(TableMetadata {
        name: table_name,
        columns,
        partition_key: partition_keys,
        cluster_key: vec![],
    }))
}

fn derive_filters(
    select: &Box<Select>,
    table: &TableMetadata,
) -> anyhow::Result<(HashMap<String, ParsedFilter>, Vec<String>)> {
    let mut filters = HashMap::new();
    let mut partition_key: Vec<String> = vec![];

    if let Some(where_clause) = &select.selection {
        match where_clause {
            Expr::BinaryOp { left, op, right } => {
                if let Expr::Identifier(ident) = &**left {
                    let column_name = ident.value.clone();
                    let is_partition_key = table.partition_key.contains(&column_name);
                    let column_metadata = table
                        .columns
                        .get(&column_name)
                        .ok_or_else(|| anyhow!("Column not found: {}", column_name))?;

                    let value = match &**right {
                        Expr::Value(val) => {
                            if is_partition_key {
                                partition_key.push(column_name.clone());
                            }

                            ParsedExpr::Literal(Value::from_sql_value(
                                column_metadata.column_type,
                                val,
                            ))
                        }
                        Expr::Identifier(id) => ParsedExpr::Column(ProjectedColumn {
                            target_column: id.value.clone(),
                            resolved_name: id.value.clone(),
                            column_metadata: table
                                .columns
                                .get(&id.value)
                                .ok_or_else(|| anyhow!("Column not found: {}", id.value))?
                                .clone(),
                        }),
                        _ => return Err(anyhow!("Unsupported filter expression")),
                    };

                    let filter_function = match op {
                        BinaryOperator::Eq => eq,
                        BinaryOperator::NotEq => neq,
                        BinaryOperator::Gt => unimplemented!("gt"),
                        BinaryOperator::GtEq => unimplemented!(),
                        BinaryOperator::Lt => unimplemented!(),
                        BinaryOperator::LtEq => unimplemented!(),
                        _ => return Err(anyhow!("Unsupported operator")),
                    };

                    filters.insert(
                        column_name.clone(),
                        ParsedFilter {
                            filter: filter_function,
                            args: vec![
                                ParsedExpr::Column(ProjectedColumn {
                                    target_column: column_name.clone(),
                                    resolved_name: column_name,
                                    column_metadata: column_metadata.clone(),
                                }),
                                value,
                            ],
                        },
                    );
                    Ok((filters, partition_key))
                } else {
                    Err(anyhow!("Left side of filter must be a column"))
                }
            }
            _ => Err(anyhow!("Unsupported where clause expression")),
        }
    } else {
        Ok((filters, partition_key))
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

async fn derive_table_metadata(
    tables: &Arc<RwLock<Tables>>,
    select: &Box<Select>,
) -> anyhow::Result<TableMetadata> {
    let table = match &select.from.first().unwrap().relation {
        TableFactor::Table { name, .. } => {
            let table_name = name.to_string();

            tables
                .read()
                .await
                .get(&table_name)
                .ok_or_else(|| anyhow::anyhow!("Table not found"))?
                .clone()
        }
        _ => unimplemented!(),
    };
    Ok(table.clone())
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
    use crate::db::schema::{ColumnMetadata, Kind, TableMetadata};
    use indexmap::IndexMap;
    use std::collections::HashMap;

    #[tokio::test]
    async fn parse_insert_valid() {
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

        let table = TableMetadata {
            name: "users".to_string(),
            columns,
            partition_key: vec!["id".to_string()],
            cluster_key: vec![],
        };

        let tables = Arc::new(RwLock::new(HashMap::new()));
        tables.write().await.insert("users".to_string(), table);

        let sql = "INSERT INTO users (id, name) VALUES (1, 'John Doe')".to_string();
        let insert = Parser::parse_sql(&CassandraDialect {}, &sql)
            .unwrap()
            .pop()
            .unwrap();

        if let Statement::Insert(insert) = insert {
            let result = parse_insert(&tables, &Box::new(insert)).await;
            assert!(result.is_ok());

            if let ParsedStatement::Insert(parsed_insert) = result.unwrap() {
                assert_eq!(parsed_insert.table.name, "users");
                assert_eq!(parsed_insert.values.len(), 2);
            } else {
                panic!("Expected ParsedStatement::Insert");
            }
        } else {
            panic!("Expected Statement::Insert");
        }
    }

    #[test]
    fn test_parse_create_table() {
        let tables = Arc::new(RwLock::new(HashMap::new()));
        let sql = "CREATE TABLE users (id smallint PRIMARY KEY, name varchar)".to_string();

        let result = tokio_test::block_on(parse(sql, &tables));

        assert!(result.is_ok());

        if let ParsedStatement::Create(table) = result.unwrap() {
            assert_eq!(table.name, "users");
            assert_eq!(table.partition_key, vec!["id"]);
            assert_eq!(table.cluster_key.len(), 0);

            let id_column = table.columns.get("id").unwrap();

            assert_eq!(id_column.name, "id");
            assert_eq!(id_column.column_type, ColumnType::Smallint);
            assert_eq!(id_column.kind, Kind::PartitionKey);

            let name_column = table.columns.get("name").unwrap();

            assert_eq!(name_column.name, "name");
            assert_eq!(name_column.column_type, ColumnType::Varchar);
            assert_eq!(name_column.kind, Kind::Regular);
        } else {
            panic!("Expected ParsedStatement::Create");
        }
    }

    #[test]
    fn test_parse() {
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
        let tables = Arc::new(RwLock::new(HashMap::new()));

        tokio_test::block_on(tables.write()).insert("users".to_string(), table);

        let sql = "SELECT id, name FROM users".to_string();

        // Call the parse function
        let result = tokio_test::block_on(parse(sql, &tables));

        // Check the result
        assert!(result.is_ok());

        match result.unwrap() {
            ParsedStatement::Select(parsed_query) => {
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
            }
            _ => {
                panic!("Expected SELECT")
            }
        }
    }
}

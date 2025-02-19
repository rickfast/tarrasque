use sqlparser::ast::{BinaryOperator, Expr, SelectItem, SetExpr, Statement};
use sqlparser::parser::Parser;
use crate::db::dialect::CassandraDialect;
use crate::db::execution::{ExecutionGraph, Op};

// Function to check if the WHERE clause uses a partition key
fn analyze_where_clause(expr: &Expr, partition_key: &str, clustering_key: Option<&str>) -> (bool, bool) {
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

// Build Execution Graph
fn build_execution_graph(sql: &str, partition_key: &str, clustering_key: Option<&str>) -> ExecutionGraph {
    let dialect = CassandraDialect {}; // Use Generic SQL parser
    let statements = Parser::parse_sql(&dialect, sql).expect("Failed to parse SQL");

    let mut graph = ExecutionGraph::new();
    let mut last_node = None;

    for statement in statements {
        if let Statement::Query(query) = statement {
            if let SetExpr::Select(select) = *query.body {

                // Step 1: Determine if it's a partition lookup or full table scan
                let mut is_partition_lookup = false;
                let mut is_clustering_used = false;

                if let Some(selection) = &select.selection {
                    (is_partition_lookup, is_clustering_used) = analyze_where_clause(selection, partition_key, clustering_key);
                }

                let table_name = select
                    .from
                    .get(0)
                    .map(|t| t.relation.to_string())
                    .unwrap_or_else(|| "UNKNOWN_TABLE".to_string());

                let scan_operation = if is_partition_lookup {
                    Op::PartitionLookup
                } else if is_clustering_used {
                    Op::Filter
                } else {
                    Op::TableScan
                };

                let scan_id = graph.add_node(scan_operation, vec![]);
                last_node = Some(scan_id);

                // Step 2: Filter Operation (if applicable)
                if let Some(selection) = select.selection {
                    let filter_id = graph.add_node(Op::Filter, vec![scan_id]);
                    last_node = Some(filter_id);
                }

                // Step 3: Projection (SELECT columns)
                let projection_columns: Vec<String> = select
                    .projection
                    .iter()
                    .map(|item| match item {
                        SelectItem::UnnamedExpr(Expr::Identifier(ident)) => ident.to_string(),
                        _ => "UNKNOWN".to_string(),
                    })
                    .collect();

                let projection_id = graph.add_node(
                    Op::Projection {columns: projection_columns.clone()},
                    last_node.map_or(vec![], |id| vec![id]),
                );
                last_node = Some(projection_id);
            }
        }
    }

    graph
}
use std::collections::HashMap;
use std::hash::Hasher;
use fjall::{Config, PersistMode, Keyspace, PartitionCreateOptions};
use crate::db::data::Row;
use crate::serde::reader::Value;

// #[derive(Debug)]
// pub enum Op {
//     TableScan {
//         table_name: String,
//     },
//     KeyLookup {
//         partition_key: Key,
//         clustering_key: Option<Key>
//     },
//     PartitionScan,
//     Filter {
//
//     },
//     Projection {
//         columns: HashMap<String, Projection>,
//     },
// }
//
// type Function = fn(&Value) -> Value;
//
// #[derive(Debug)]
// pub enum Projection {
//     Function(Function),
//     Column(String)
// }
//
// pub struct ExecutionContext {
//     db: Keyspace,
//     cursor: Option<Box<dyn Iterator<Item=Row>>>
// }
//
// #[derive(Debug)]
// pub struct ExecutionNode {
//     id: usize,
//     operation: Op, // TABLE_SCAN, PARTITION_LOOKUP, FILTER, etc.
//     children: Vec<usize>,
// }
//
// impl ExecutionNode {
//     fn execute(&self, context: &mut ExecutionContext) {
//         match &self.operation {
//             Op::TableScan { table_name} => {
//                 match context.db.open_partition("", PartitionCreateOptions::default()) {
//                     Ok(partition) => {
//                         context.cursor = Some(Box::new(partition.values().map(|bytes| {
//                             Row::try_from(bytes.unwrap()).unwrap()
//                         })));
//                     },
//                     Err(e) => panic!("Error opening partition: {}", e)
//                 }
//             },
//             Op::Filter => {
//                 match &context.cursor {
//                     Some(cursor) => {
//                         for row in cursor {
//
//                         }
//                     },
//                     None => {}
//                 }
//             },
//             Op::Projection { columns } => {
//                 if let Some(cursor) = &context.cursor {
//                     context.cursor = Some(Box::new(cursor.map(|row| {
//                         let mut new_row = Row::new();
//                         for column in columns {
//                             new_row.insert(column, row.get(column));
//                         }
//                         new_row
//                     })));
//                 }
//             },
//             _ => unimplemented!()
//         }
//     }
// }
//
// // Execution Graph Structure
// #[derive(Debug)]
// pub struct ExecutionGraph {
//     nodes: Vec<ExecutionNode>,
// }
//
// impl ExecutionGraph {
//     pub fn new() -> Self {
//         ExecutionGraph { nodes: Vec::new() }
//     }
//
//     pub fn add_node(&mut self, operation: Op, children: Vec<usize>) -> usize {
//         let id = self.nodes.len();
//
//         self.nodes.push(ExecutionNode {
//             id,
//             operation,
//             children,
//         });
//
//         id
//     }
// }

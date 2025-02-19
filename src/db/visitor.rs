use crate::db::data::ColumnType;
use sled::{Db, Tree};
use sqlparser::ast::{ColumnDef, Expr, TableFactor, Visitor};
use std::collections::HashMap;
use std::fmt::Pointer;
use std::hash::Hash;
use std::ops::ControlFlow;

struct CqlVisitor {
    keyspace: Keyspace,
    tables: HashMap<String, Tree>,
    db: Db,
    columns: Vec<ColumnDef>,
}

struct Keyspace {
    name: String,
    tables: HashMap<String, TableMetadata>,
}

struct TableMetadata {
    keyspace: String,
    name: String,
    partition_key: Key,
    cluster_key: Option<Key>,
    columns: HashMap<String, ColumnMetadata>
}

enum Key {
    Single(String),
    Composite(Vec<String>)
}

enum Kind {
    PartitionKey,
    Clustering,
    Regular,
    Static
}

struct ColumnMetadata {
    name: String,
    column_type: ColumnType,
    kind: Kind
}

struct SelectedColumn {
    column_metadata: ColumnMetadata
}

/*
,
            SetExpr::Select(select) => {
                select.projection.iter().for_each(|item| {
                    match item {
                        SelectItem::ExprWithAlias { expr, alias } => {
                            print!("Alias => {:?}, ", alias);
                        },
                        _ => {}
                    }
                });
            },
 */

struct QuerySpec {
    tables: HashMap<String, TableMetadata>,
    selection: Vec<SelectedColumn>,
}

impl Visitor for CqlVisitor {
    type Break = ();

    fn pre_visit_table_factor(&mut self, _table_factor: &TableFactor) -> ControlFlow<()> {
        let table_name = match &_table_factor {
            TableFactor::Table { name, alias, .. } => {
                match alias {
                    Some(alias) => alias.name.value.clone(),
                    None => name.to_string()
                }
            },
            _ => unreachable!()
        };

        println!("Table name => {:?}", table_name);

        match self.tables.get(&table_name) {
            Some(_) => ControlFlow::Continue(()),
            None => {
                let tree: Tree = self.db.open_tree(&table_name).unwrap();
                self.tables.insert(table_name, tree);
                ControlFlow::Continue(())
            }
        }
    }

    fn post_visit_expr(&mut self, _expr: &Expr) -> ControlFlow<Self::Break> {
        println!("{:?}", _expr);
        match _expr {
            Expr::Identifier(ident) => {
                println!("Identifier => {:?}", ident)
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

#[cfg(test)]
mod tests {
    use crate::db::dialect::CassandraDialect;
    use crate::db::visitor::{CqlVisitor, Key, Keyspace, TableMetadata};
    use sqlparser::ast::Visit;
    use sqlparser::parser::Parser;
    use std::collections::HashMap;

    #[test]
    fn test_visitor() {
        let sql = "select bx.a, abs(bx.b), bx.c as d from blah bx";

        /// let vikings = HashMap::from([
        ///     (Viking::new("Einar", "Norway"), 25),
        ///     (Viking::new("Olaf", "Denmark"), 24),
        ///     (Viking::new("Harald", "Iceland"), 12),
        /// ]);

        let mut cql_visitor = CqlVisitor {
            keyspace: Keyspace {
                name: String::from(""),
                tables: HashMap::from([
                    (String::from("blah"), TableMetadata {
                        name: String::from("blah"),
                        keyspace: String::from(""),
                        cluster_key: None,
                        partition_key: Key::Single(String::from("")),
                        columns: HashMap::from([
                            ()
                        ])
                    })
                ]),
            },
            tables: HashMap::new(),
            db: sled::open("/tmp/db").unwrap(),
            columns: Vec::new(),
        };
        let statements = match Parser::parse_sql(&CassandraDialect {}, sql) {
            Ok(statements) => statements,
            Err(parse_error) => panic!("{}", parse_error)
        };
        let control_flow = statements.first().unwrap().visit(&mut cql_visitor);


    }
}

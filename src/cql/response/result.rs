use crate::db::data::{ColumnType, Row};
use crate::serde::writer::{int, string};
use bitflags::bitflags;
use bytes::BytesMut;

#[derive(Debug, Clone)]
pub(crate) enum Result {
    Void,
    SetKeyspace(String),
    Rows(Rows),
}

#[derive(Debug, Clone)]
struct GlobalTableSpec {
    keyspace: String,
    table: String,
}

#[derive(Debug, Clone)]
struct ColumnSpec {
    keyspace: Option<String>,
    table: Option<String>,
    name: String,
    column_type: ColumnType,
}

bitflags! {
    /// Represents a set of flags.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    struct Flags: i32 {
        const GLOBAL_TABLES_SPEC = 0x01;
        const HAS_PAGING_STATE = 0x02;
        const NO_METADATA = 0x03;
    }
}

#[derive(Debug, Clone)]
struct Metadata {
    flags: Flags,
    paging_state: Option<String>,
    column_count: i32,
    global_table_spec: Option<GlobalTableSpec>,
    column_specs: Vec<ColumnSpec>,
}

#[derive(Debug, Clone)]
struct Rows {
    metadata: Metadata,
    row_count: i32,
    rows: Vec<Row>,
}

pub(crate) fn encode(src: Result, dst: &mut BytesMut) -> anyhow::Result<()> {
    match src {
        Result::Void => int!(dst, 01),
        Result::SetKeyspace(keyspace) => string!(dst, keyspace),
        Result::Rows(rows) => {
            let metadata = rows.metadata;
            let flags = metadata.flags;

            int!(dst, flags.bits());
            int!(dst, metadata.column_count);

            match metadata.global_table_spec {
                Some(spec) => {
                    string!(dst, spec.keyspace);
                    string!(dst, spec.table);
                }
                None => {
                    unimplemented!()
                }
            }
        }
    };

    Ok(())
}

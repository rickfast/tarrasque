use crate::db::data::{ColumnType, Row, Value};
use crate::serde::writer::{bool, bytes, int, string};
use bitflags::bitflags;
use bytes::BytesMut;

#[derive(Debug, Clone)]
pub(crate) enum Result {
    Void,
    SetKeyspace(String),
    Rows {
        metadata: Metadata,
        row_count: i32,
        rows: Vec<Row>,
    },
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
    pub struct Flags: i32 {
        const GLOBAL_TABLES_SPEC = 0x01;
        const HAS_PAGING_STATE = 0x02;
        const NO_METADATA = 0x03;
    }
}

#[derive(Debug, Clone)]
pub struct Metadata {
    flags: Flags,
    paging_state: Option<String>,
    column_count: i32,
    global_table_spec: Option<GlobalTableSpec>,
    column_specs: Vec<ColumnSpec>,
}

impl Metadata {
    pub fn new(flags: Flags, column_count: i32) -> Metadata {
        Metadata {
            flags,
            paging_state: None,
            column_count,
            global_table_spec: None,
            column_specs: vec![],
        }
    }
}

pub(crate) fn encode(src: Result, dst: &mut BytesMut) -> anyhow::Result<()> {
    match src {
        Result::Void => int!(dst, 01),
        Result::SetKeyspace(keyspace) => string!(dst, keyspace),
        Result::Rows {
            metadata,
            row_count: _,
            rows,
        } => {
            let metadata = metadata;
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

            for row in rows {
                for column in row.columns {
                    match column {
                        None => {
                            int!(dst, 0);
                        }
                        Some(column) => match column {
                            Value::Int(value) => bytes!(dst, value.to_be_bytes().as_slice()),
                            Value::Ascii(value) => bytes!(dst, value.as_slice()),
                            Value::Bigint(value) => bytes!(dst, value.to_be_bytes().as_slice()),
                            Value::Blob(value) => bytes!(dst, value.as_slice()),
                            Value::Boolean(value) => bytes!(dst, &[value as u8]),
                            Value::Counter(_) => {}
                            Value::Decimal(_) => {}
                            Value::Double(value) => bytes!(dst, value.to_be_bytes().as_slice()),
                            Value::Float(value) => bytes!(dst, value.to_be_bytes().as_slice()),
                            Value::Timestamp(value) => bytes!(dst, value.to_be_bytes().as_slice()),
                            Value::Uuid(uuid) => bytes!(dst, uuid.as_bytes().as_slice()),
                            Value::Varchar(value) => bytes!(dst, value.as_bytes()),
                            Value::Varint(_) => {}
                            Value::Timeuuid(_) => {}
                            Value::Inet(_) => {}
                            Value::Date(value) => bytes!(dst, value.to_be_bytes().as_slice()),
                            Value::Time(_) => {}
                            Value::Smallint(value) => bytes!(dst, value.to_be_bytes().as_slice()),
                            Value::Tinyint(value) => bytes!(dst, value.to_be_bytes().as_slice()),
                        },
                    }
                }
            }
        }
    };

    Ok(())
}

use crate::cql::Operation;
use crate::serde::reader::{byte, bytes, int, long, long_string, short, string, value, Value};
use bitflags::{bitflags, Flags};
use bytes::Bytes;
use std::collections::HashMap;
use tokio_util::bytes::BytesMut;

#[derive(Debug, Clone)]
pub struct Query {
    pub query: String,
    pub query_options: QueryOptions,
}

#[derive(Debug, Clone)]
pub(crate) struct QueryOptions {
    consistency: Consistency,
    values: Option<Values>,
    skip_metadata: bool,
    page_size: Option<i32>,
    paging_state: Option<Bytes>,
    timestamp: Option<i64>,
}

#[derive(Debug, Clone)]
enum Values {
    Unnamed(Vec<Value>),
    Named(HashMap<String, Value>),
}

#[derive(Debug, Clone)]
enum Consistency {
    Any,
    One,
    Two,
    Three,
    Quorum,
    All,
    LocalQuorum,
    EachQuorum,
    Serial,
    LocalSerial,
    LocalOne,
}

bitflags! {
    /// Represents a set of flags.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    struct QueryFlags: u8 {
        const VALUES = 0x01;
        const SKIP_METADATA = 0x02;
        const PAGE_SIZE = 0x03;
        const PAGING_STATE = 0x04;
        const SERIAL_CONSISTENCY = 0x05;
        const TIMESTAMPS = 0x06;
        const NAMES_FOR_VALUES = 0x07;
    }
}

pub(crate) fn decode(src: &mut BytesMut) -> Operation {
    let query = long_string!(src);
    let consistency = map_consistency(short!(src));
    let flags = QueryFlags::from_bits_truncate(byte!(src));

    let values: Option<Values> = if flags.contains(QueryFlags::VALUES) {
        let num_values = short!(src);
        let with_names = flags.contains(QueryFlags::NAMES_FOR_VALUES);
        let mut i = 0;

        if with_names {
            let mut items: HashMap<String, Value> = HashMap::new();

            while i < num_values as usize {
                items.insert(string!(src), value!(src));
                i = i + 1
            }

            Some(Values::Named(items))
        } else {
            let mut items: Vec<Value> = Vec::new();

            while i < num_values as usize {
                items.push(value!(src));
                i = i + 1
            }

            Some(Values::Unnamed(items))
        }
    } else {
        None
    };
    let skip_metadata = flags.contains(QueryFlags::SKIP_METADATA);
    let page_size = if flags.contains(QueryFlags::PAGE_SIZE) {
        Some(int!(src))
    } else {
        None
    };
    let paging_state: Option<Bytes> = if flags.contains(QueryFlags::PAGING_STATE) {
        bytes!(src)
    } else {
        None
    };
    let timestamp = if flags.contains(QueryFlags::TIMESTAMPS) {
        Some(long!(src))
    } else {
        None
    };

    Operation::Query(Query {
        query,
        query_options: QueryOptions {
            consistency,
            values,
            skip_metadata,
            page_size,
            paging_state,
            timestamp,
        },
    })
}

fn map_consistency(value: u16) -> Consistency {
    match value {
        0x0000 => Consistency::Any,
        0x0001 => Consistency::One,
        0x0002 => Consistency::Two,
        0x0003 => Consistency::Three,
        0x0004 => Consistency::Quorum,
        0x0005 => Consistency::All,
        0x0006 => Consistency::LocalQuorum,
        0x0007 => Consistency::EachQuorum,
        0x0008 => Consistency::Serial,
        0x0009 => Consistency::LocalSerial,
        0x000A => Consistency::LocalOne,
        _ => panic!("invalid consistency value"),
    }
}

use crate::cql::Operation;
use crate::serde::reader::{short, string, string_map};
use bytes::BytesMut;
use std::collections::HashMap;

pub(crate) fn decode(src: &mut BytesMut) -> Operation {
    let options = string_map!(src);

    Operation::Startup(options)
}

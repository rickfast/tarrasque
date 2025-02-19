use crate::serde::writer::{string, string_map};
use bytes::BytesMut;
use std::collections::HashMap;

pub(crate) fn encode(src: &HashMap<&str, &str>, dst: &mut BytesMut) -> anyhow::Result<()> {
    string_map!(dst, src);

    Ok(())
}

use crate::serde::writer::{int, string};
use bytes::BytesMut;

#[derive(Debug, Clone)]
pub struct Error {
    code: i32,
    message: String,
}

impl Error {
    pub fn new(code: i32, message: String) -> Error {
        Error { code, message }
    }
}

pub(crate) fn encode(src: Error, dst: &mut BytesMut) -> anyhow::Result<()> {
    int!(dst, src.code);
    string!(dst, src.message);

    Ok(())
}

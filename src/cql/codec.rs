use crate::cql::header::{Header, MessageDirection, ProtocolVersion};
use crate::cql::operation::Operation;
use crate::cql::{request, response};
use bytes::{BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

pub struct CqlFrameCodec {}

impl CqlFrameCodec {
    pub fn new() -> Self {
        CqlFrameCodec {}
    }
}

impl Encoder<Operation> for CqlFrameCodec {
    type Error = anyhow::Error;

    fn encode(&mut self, item: Operation, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let mut payload = BytesMut::new();

        let op_code = item.op_code();

        println!("Encoding operation: {:?}", op_code);

        match item {
            Operation::Result(result) => response::result::encode(result, &mut payload),
            Operation::Error(error) => response::error::encode(error, &mut payload),
            Operation::Supported(values) => response::supported::encode(&values, &mut payload),
            Operation::Ready => Ok(()),
            _ => unimplemented!(),
        }?;

        let payload_len = payload.len() as i32;
        let header = Header::new(
            ProtocolVersion::V4,
            MessageDirection::Response,
            0,
            0,
            op_code,
            payload_len,
        );

        dst.reserve(9);

        let mut header_bytes = BytesMut::new();

        header.to_bytes(&mut header_bytes);
        dst.extend_from_slice(&header_bytes);
        dst.reserve(payload_len as usize);
        dst.put(payload);

        Ok(())
    }
}

impl Decoder for CqlFrameCodec {
    type Item = Operation;
    type Error = anyhow::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 9 {
            Ok(None)
        } else {
            let body_bytes = &mut src.split_off(9usize);
            let header = Header::from_bytes(src);

            println!("Decoding operation: {:?}", header.op_code);

            if body_bytes.len() < header.body_length as usize {
                Ok(None)
            } else {
                let op = match header.op_code {
                    0x01 => request::startup::decode(body_bytes),
                    0x05 => Operation::Options,
                    0x07 => request::query::decode(body_bytes),
                    _ => panic!("invalid operation {:?}", header.op_code),
                };

                Ok(Some(op))
            }
        }
    }
}

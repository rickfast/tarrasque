use bytes::{BufMut, BytesMut};

pub struct Header {
    version: u8,
    flags: u8,
    stream_id: u16,
    pub op_code: u8,
    pub body_length: i32,
}

const PROTOCOL_VERSION_MASK: u8 = 0x7F;
const MESSAGE_DIRECTION_MASK: u8 = 0x80;

impl Header {
    pub fn protocol_version(&self) -> ProtocolVersion {
        let byte = self.version & PROTOCOL_VERSION_MASK;
        ProtocolVersion::from_u8(byte)
    }

    pub fn message_direction(&self) -> MessageDirection {
        match self.version & MESSAGE_DIRECTION_MASK {
            0x00 => MessageDirection::Request,
            0x80 => MessageDirection::Response,
            _ => unimplemented!(),
        }
    }

    pub fn new(
        protocol_version: ProtocolVersion,
        message_direction: MessageDirection,
        flags: u8,
        stream_id: u16,
        op_code: u8,
        body_length: i32,
    ) -> Header {
        let version: u8 = match message_direction {
            MessageDirection::Request => match protocol_version {
                ProtocolVersion::V4 => 0x04,
                ProtocolVersion::V5 => 0x05,
                _ => unimplemented!(),
            },
            MessageDirection::Response => match protocol_version {
                ProtocolVersion::V4 => 0x84,
                ProtocolVersion::V5 => 0x85,
                _ => unimplemented!(),
            },
        };

        Header {
            version,
            flags,
            stream_id,
            op_code,
            body_length,
        }
    }

    pub fn from_bytes(src: &BytesMut) -> Self {
        Header {
            version: src[0],
            flags: src[1],
            stream_id: u16::from_be_bytes([src[2], src[3]]),
            op_code: src[4],
            body_length: i32::from_be_bytes([src[5], src[6], src[7], src[8]]),
        }
    }

    pub fn to_bytes(&self, buf: &mut BytesMut) {
        buf.put_u8(self.version);
        buf.put_u8(self.flags);
        buf.put_u16(self.stream_id);
        buf.put_u8(self.op_code);
        buf.put_i32(self.body_length);
    }
}
pub enum ProtocolVersion {
    V1,
    V2,
    V3,
    V4,
    V5,
}

impl ProtocolVersion {
    fn from_u8(value: u8) -> ProtocolVersion {
        match value {
            0x01 => ProtocolVersion::V1,
            0x02 => ProtocolVersion::V2,
            0x03 => ProtocolVersion::V3,
            0x04 => ProtocolVersion::V4,
            _ => ProtocolVersion::V5,
        }
    }
}

pub enum MessageDirection {
    Request,
    Response,
}

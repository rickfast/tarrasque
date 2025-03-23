macro_rules! int {
    ($bytes:expr,$value:expr) => {{
        $bytes.reserve(4 as usize);
        $bytes.extend_from_slice(i32::to_be_bytes($value).as_slice());
    }};
}

macro_rules! long {
    ($bytes:expr,$value:expr) => {{
        $bytes.reserve(8 as usize);
        $bytes.extend_from_slice(i64::to_be_bytes($value).as_slice());
    }};
}

macro_rules! unsigned_short {
    ($bytes:expr,$value:expr) => {{
        $bytes.reserve(2 as usize);
        $bytes.extend_from_slice(u16::to_be_bytes($value).as_slice());
    }};
}

macro_rules! short {
    ($bytes:expr,$value:expr) => {{
        $bytes.reserve(2 as usize);
        $bytes.extend_from_slice(i16::to_be_bytes($value).as_slice());
    }};
}

macro_rules! tinyint {
    ($bytes:expr,$value:expr) => {{
        $bytes.reserve(1 as usize);
        $bytes.extend_from_slice(i8::to_be_bytes($value).as_slice());
    }};
}

pub(crate) use int;
pub(crate) use long;
pub(crate) use unsigned_short;
pub(crate) use tinyint;
pub(crate) use short;

macro_rules! float {
    ($bytes:expr, $value:expr) => {{
        $bytes.reserve(4 as usize);
        $bytes.extend_from_slice(f32::to_be_bytes($value).as_slice());
    }};
}

macro_rules! double {
    ($bytes:expr, $value:expr) => {{
        $bytes.reserve(8 as usize);
        $bytes.extend_from_slice(f64::to_be_bytes($value).as_slice());
    }};
}

pub(crate) use float;
pub(crate) use double;

macro_rules! bytes {
    ($bytes:expr,$value:expr) => {{
        let byte_length = $value.len() as u16;
        let length = 2u16 + $value.len() as u16;
        $bytes.reserve(length as usize);
        $bytes.extend_from_slice(u16::to_be_bytes(byte_length).as_slice());
        $bytes.extend_from_slice($value);
    }};
}

macro_rules! string {
    ($bytes:expr,$value:expr) => {{
        let byte_length = $value.len() as u16;
        let length = 2u16 + $value.len() as u16;
        $bytes.reserve(length as usize);
        $bytes.extend_from_slice(u16::to_be_bytes(byte_length).as_slice());
        $bytes.extend_from_slice($value.as_bytes());
    }};
}

pub use bytes;
pub(crate) use string;


macro_rules! string_map {
    ($bytes:expr,$value:expr) => {{
        let length = $value.len() as u16;
        $bytes.reserve(2 as usize);
        $bytes.extend_from_slice(u16::to_be_bytes(length).as_slice());

        for (key, value) in $value {
            string!($bytes, key);
            string!($bytes, value);
        }
    }};
}

pub(crate) use string_map;

macro_rules! bool {
    ($bytes:expr, $value:expr) => {{
        $bytes.reserve(1 as usize);
        $bytes.extend_from_slice(&[$value as u8]);
    }};
}

pub use bool;

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn test_int_macro() {
        let mut bytes = BytesMut::new();
        int!(bytes, 42);
        assert_eq!(bytes, &42i32.to_be_bytes()[..]);
    }

    #[test]
    fn test_long_macro() {
        let mut bytes = BytesMut::new();
        long!(bytes, 42);
        assert_eq!(bytes, &42i64.to_be_bytes()[..]);
    }

    #[test]
    fn test_short_macro() {
        let mut bytes = BytesMut::new();
        unsigned_short!(bytes, 42);
        assert_eq!(bytes, &42u16.to_be_bytes()[..]);
    }

    #[test]
    fn test_tinyint_macro() {
        let mut bytes = BytesMut::new();
        tinyint!(bytes, 42);
        assert_eq!(bytes, &42i8.to_be_bytes()[..]);
    }

    #[test]
    fn test_bytes_macro() {
        let mut bytes = BytesMut::new();
        let value = b"hello";
        bytes!(bytes, value);

        let (size, content) = bytes.split_at(2);

        assert_eq!(size, &5u16.to_be_bytes()[..]);
        assert_eq!(content.to_vec(), value.to_vec());
    }

    #[test]
    fn test_string_macro() {
        let mut bytes = BytesMut::new();
        let value = "hello";
        string!(bytes, value);
        let mut expected = BytesMut::new();
        expected.extend_from_slice(&(value.len() as u16).to_be_bytes());
        expected.extend_from_slice(value.as_bytes());
        assert_eq!(bytes, expected);
    }

    #[test]
    fn test_bool_macro() {
        let mut bytes = BytesMut::new();
        bool!(bytes, true);
        assert_eq!(bytes, &[1u8][..]);

        let mut bytes = BytesMut::new();
        bool!(bytes, false);
        assert_eq!(bytes, &[0u8][..]);
    }

    // #[test]
    // fn test_string_map_macro() {
    //     let mut bytes = BytesMut::new();
    //     let mut map = std::collections::HashMap::new();
    //     map.insert("key1".to_string(), "value1".to_string());
    //     map.insert("key2".to_string(), "value2".to_string());
    //     string_map!(bytes, map);
    //     let mut expected = BytesMut::new();
    //     expected.extend_from_slice(&(map.len() as u16).to_be_bytes());
    //     for (key, value) in &map {
    //         expected.extend_from_slice(&(key.len() as u16).to_be_bytes());
    //         expected.extend_from_slice(key.as_bytes());
    //         expected.extend_from_slice(&(value.len() as u16).to_be_bytes());
    //         expected.extend_from_slice(value.as_bytes());
    //     }
    //     assert_eq!(bytes, expected);
    // }
}
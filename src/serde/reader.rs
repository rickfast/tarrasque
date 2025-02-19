use bytes::Bytes;

macro_rules! short {
    ($bytes:expr) => {
        u16::from_be_bytes($bytes.split_to(2)[0..=1].try_into().unwrap())
    };
}

macro_rules! long {
    ($bytes:expr) => {
        i64::from_be_bytes($bytes.split_to(8)[0..=7].try_into().unwrap())
    };
}

macro_rules! int {
    ($bytes:expr) => {
        i32::from_be_bytes($bytes.split_to(4)[0..=3].try_into().unwrap())
    };
}

macro_rules! byte {
    ($bytes:expr) => {
        $bytes.split_to(1)[0]
    };
}

pub(crate) use byte;
pub(crate) use int;
pub(crate) use long;
pub(crate) use short;

macro_rules! string {
    ($bytes:expr) => {{
        let length = short!($bytes) as usize;
        String::from_utf8_lossy(&$bytes.split_to(length)[0..length]).to_string()
    }};
}

macro_rules! long_string {
    ($bytes:expr) => {{
        let length = int!($bytes) as usize;
        String::from_utf8_lossy(&$bytes.split_to(length)[0..length]).to_string()
    }};
}

pub(crate) use long_string;
pub(crate) use string;

#[derive(Debug, Clone)]
pub(crate) enum Value {
    Null,
    NotSet,
    Set { bytes: Bytes },
    Error,
}

macro_rules! value {
    ($bytes:expr) => {{
        let length = int!($bytes);
        match length {
            -1 => Value::Null,
            -2 => Value::NotSet,
            _ => {
                if length < -2 {
                    Value::Error
                } else {
                    Value::Set {
                        bytes: $bytes.split_to(length as usize).freeze(),
                    }
                }
            }
        }
    }};
}

pub(crate) use value;

macro_rules! string_map {
    ($bytes:expr) => {{
        let length = short!($bytes) as usize;
        let mut map: HashMap<String, String> = HashMap::new();

        for n in 0..length {
            map.insert(string!($bytes), string!($bytes));
        }

        map
    }};
}

pub(crate) use string_map;

macro_rules! bytes {
    ($bytes:expr) => {{
        let length = int!($bytes) as usize;

        if (length < 0) {
            None
        } else {
            Some($bytes.split_to(length as usize).freeze())
        }
    }};
}

pub(crate) use bytes;

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn test_string() {
        let mut str = b"Farts".to_vec();
        let mut len = 5u16.to_be_bytes().to_vec();

        len.append(&mut str);

        let mut bytes = BytesMut::from(len.as_slice());

        let string = string!(bytes);

        assert_eq!(string, String::from("Farts"));
    }

    #[test]
    fn test_numeric_reader() {
        let bytes = 12i32.to_be_bytes();
        let mut src = BytesMut::from(bytes.as_slice());
        let read = int!(&mut src);

        assert_eq!(12i32, read);
        assert_eq!(0, src.len());

        let bytes = 12i64.to_be_bytes();
        let mut src = BytesMut::from(bytes.as_slice());
        let read = long!(&mut src);

        assert_eq!(12i64, read);
        assert_eq!(0, src.len());

        let bytes = 12u16.to_be_bytes();
        let mut src = BytesMut::from(bytes.as_slice());
        let read = short!(&mut src);

        assert_eq!(12u16, read);
        assert_eq!(0, src.len());
    }

    // #[test]
    // fn test_string_reader() {
    //     let mut length_bytes = 11u16.to_be_bytes().to_vec();
    //     let mut bytes = String::from("hello world").into_bytes();
    //
    //     length_bytes.append(&mut bytes);
    //
    //     assert_eq!(length_bytes.len(), 13);
    //
    //     let mut src = BytesMut::from(&length_bytes[..]);
    //     let read = read_string(&mut src);
    //
    //     assert_eq!(String::from("hello world"), read);
    // }
}

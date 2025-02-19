macro_rules! int {
    ($bytes:expr,$value:expr) => {{
        $bytes.reserve(4 as usize);
        $bytes.extend_from_slice(i32::to_be_bytes($value).as_slice());
    }};
}

macro_rules! short {
    ($bytes:expr,$value:expr) => {{
        $bytes.reserve(2 as usize);
        $bytes.extend_from_slice(u16::to_be_bytes($value).as_slice());
    }};
}

macro_rules! tinyint {
    ($bytes:expr,$value:expr) => {{
        $bytes.reserve(2 as usize);
        $bytes.extend_from_slice(i16::to_be_bytes($value).as_slice());
    }};
}

pub(crate) use int;

macro_rules! string {
    ($bytes:expr,$value:expr) => {{
        let length = 2u16 + $value.len() as u16;
        $bytes.reserve(length as usize);
        $bytes.extend_from_slice(u16::to_be_bytes(length).as_slice());
        $bytes.extend_from_slice($value.as_bytes());
    }};
}

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

use fjall::Slice;
use sqlparser::ast::Value as SqlValue;
use std::fmt::Debug;
use uuid::Uuid;

#[derive(Debug, Copy, Clone, PartialEq)]
pub(crate) enum ColumnType {
    Ascii,
    Bigint,
    Blob,
    Boolean,
    Counter,
    Decimal,
    Double,
    Float,
    Int,
    Timestamp,
    Uuid,
    Varchar,
    Varint,
    Timeuuid,
    Inet,
    Date,
    Time,
    Smallint,
    Tinyint,
}

impl ColumnType {
    pub fn from_cql_type(type_str: String) -> Option<ColumnType> {
        match type_str.to_lowercase().as_str() {
            "ascii" => Some(ColumnType::Ascii),
            "bigint" => Some(ColumnType::Bigint),
            "blob" => Some(ColumnType::Blob),
            "boolean" => Some(ColumnType::Boolean),
            "counter" => Some(ColumnType::Counter),
            "decimal" => Some(ColumnType::Decimal),
            "double" => Some(ColumnType::Double),
            "float" => Some(ColumnType::Float),
            "int" => Some(ColumnType::Int),
            "timestamp" => Some(ColumnType::Timestamp),
            "uuid" => Some(ColumnType::Uuid),
            "text" | "varchar" => Some(ColumnType::Varchar),
            "varint" => Some(ColumnType::Varint),
            "timeuuid" => Some(ColumnType::Timeuuid),
            "inet" => Some(ColumnType::Inet),
            "date" => Some(ColumnType::Date),
            "time" => Some(ColumnType::Time),
            "smallint" => Some(ColumnType::Smallint),
            "tinyint" => Some(ColumnType::Tinyint),
            _ => None,
        }
    }
}

const ASCII_TYPE_ID: u16 = 0x0001;
const BIGINT_TYPE_ID: u16 = 0x0002;
const BLOB_TYPE_ID: u16 = 0x0003;
const BOOLEAN_TYPE_ID: u16 = 0x0004;
const COUNTER_TYPE_ID: u16 = 0x0005;
const DECIMAL_TYPE_ID: u16 = 0x0006;
const DOUBLE_TYPE_ID: u16 = 0x0007;
const FLOAT_TYPE_ID: u16 = 0x0008;
const INT_TYPE_ID: u16 = 0x0009;
const TIMESTAMP_TYPE_ID: u16 = 0x000B;
const UUID_TYPE_ID: u16 = 0x000C;
const VARCHAR_TYPE_ID: u16 = 0x000D;
const VARINT_TYPE_ID: u16 = 0x000E;
const TIMEUUID_TYPE_ID: u16 = 0x000F;
const INET_TYPE_ID: u16 = 0x0010;
const DATE_TYPE_ID: u16 = 0x0011;
const TIME_TYPE_ID: u16 = 0x0012;
const SMALLINT_TYPE_ID: u16 = 0x0013;
const TINYINT_TYPE_ID: u16 = 0x0014;

impl ColumnType {
    pub fn type_identifier(&self) -> u16 {
        match self {
            ColumnType::Ascii => ASCII_TYPE_ID,
            ColumnType::Bigint => BIGINT_TYPE_ID,
            ColumnType::Blob => BLOB_TYPE_ID,
            ColumnType::Boolean => BOOLEAN_TYPE_ID,
            ColumnType::Counter => COUNTER_TYPE_ID,
            ColumnType::Decimal => DECIMAL_TYPE_ID,
            ColumnType::Double => DOUBLE_TYPE_ID,
            ColumnType::Float => FLOAT_TYPE_ID,
            ColumnType::Int => INT_TYPE_ID,
            ColumnType::Timestamp => TIMESTAMP_TYPE_ID,
            ColumnType::Uuid => UUID_TYPE_ID,
            ColumnType::Varchar => VARCHAR_TYPE_ID,
            ColumnType::Varint => VARINT_TYPE_ID,
            ColumnType::Timeuuid => TIMEUUID_TYPE_ID,
            ColumnType::Inet => INET_TYPE_ID,
            ColumnType::Date => DATE_TYPE_ID,
            ColumnType::Time => TIME_TYPE_ID,
            ColumnType::Smallint => SMALLINT_TYPE_ID,
            ColumnType::Tinyint => TINYINT_TYPE_ID,
        }
    }
}

#[derive(Debug, Clone)]
pub enum Value {
    Ascii(Vec<u8>),
    Bigint(i64),
    Blob(Vec<u8>),
    Boolean(bool),
    Counter(i64),
    Decimal(Vec<u8>), // Typically represented as a byte array
    Double(f64),
    Float(f32),
    Int(i32),
    Timestamp(i64), // Typically represented as a Unix timestamp
    Uuid(Uuid),
    Varchar(String),
    Varint(Vec<u8>), // Typically represented as a byte array
    Timeuuid(Uuid),
    Inet(Vec<u8>), // Typically represented as a byte array
    Date(i32),     // Typically represented as the number of days since the epoch
    Time(i64),     // Typically represented as the number of nanoseconds since midnight
    Smallint(i16),
    Tinyint(i8),
}

impl Value {
    pub fn from_sql_value(column_type: ColumnType, value: &SqlValue) -> Option<Value> {
        match value {
            SqlValue::Number(num, _) => match column_type {
                ColumnType::Bigint => Some(Value::Bigint(num.parse::<i64>().unwrap())),
                ColumnType::Int => Some(Value::Int(num.parse::<i32>().unwrap())),
                ColumnType::Smallint => Some(Value::Smallint(num.parse::<i16>().unwrap())),
                ColumnType::Tinyint => Some(Value::Tinyint(num.parse::<i8>().unwrap())),
                _ => unimplemented!(),
            },
            SqlValue::SingleQuotedString(s) => Some(Value::Varchar(s.clone())),
            SqlValue::Boolean(b) => Some(Value::Boolean(*b)),
            SqlValue::Null => None,
            _ => unimplemented!(),
        }
    }

    fn column_type(&self) -> ColumnType {
        match self {
            Value::Ascii(_) => ColumnType::Ascii,
            Value::Bigint(_) => ColumnType::Bigint,
            Value::Blob(_) => ColumnType::Blob,
            Value::Boolean(_) => ColumnType::Boolean,
            Value::Counter(_) => ColumnType::Counter,
            Value::Decimal(_) => ColumnType::Decimal,
            Value::Double(_) => ColumnType::Double,
            Value::Float(_) => ColumnType::Float,
            Value::Int(_) => ColumnType::Int,
            Value::Timestamp(_) => ColumnType::Timestamp,
            Value::Uuid(_) => ColumnType::Uuid,
            Value::Varchar(_) => ColumnType::Varchar,
            Value::Varint(_) => ColumnType::Varint,
            Value::Timeuuid(_) => ColumnType::Timeuuid,
            Value::Inet(_) => ColumnType::Inet,
            Value::Date(_) => ColumnType::Date,
            Value::Time(_) => ColumnType::Time,
            Value::Smallint(_) => ColumnType::Smallint,
            Value::Tinyint(_) => ColumnType::Tinyint,
        }
    }
}
impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Ascii(a), Value::Ascii(b)) => a == b,
            (Value::Bigint(a), Value::Bigint(b)) => a == b,
            (Value::Blob(a), Value::Blob(b)) => a == b,
            (Value::Boolean(a), Value::Boolean(b)) => a == b,
            (Value::Counter(a), Value::Counter(b)) => a == b,
            (Value::Decimal(a), Value::Decimal(b)) => a == b,
            (Value::Double(a), Value::Double(b)) => a == b,
            (Value::Float(a), Value::Float(b)) => a == b,
            (Value::Int(a), Value::Int(b)) => a == b,
            (Value::Timestamp(a), Value::Timestamp(b)) => a == b,
            (Value::Uuid(a), Value::Uuid(b)) => a == b,
            (Value::Varchar(a), Value::Varchar(b)) => a == b,
            (Value::Varint(a), Value::Varint(b)) => a == b,
            (Value::Timeuuid(a), Value::Timeuuid(b)) => a == b,
            (Value::Inet(a), Value::Inet(b)) => a == b,
            (Value::Date(a), Value::Date(b)) => a == b,
            (Value::Time(a), Value::Time(b)) => a == b,
            (Value::Smallint(a), Value::Smallint(b)) => a == b,
            (Value::Tinyint(a), Value::Tinyint(b)) => a == b,
            _ => false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Row {
    pub columns: Vec<Option<Value>>,
}

macro_rules! row {
    ($($x:expr),+ $(,)?) => {
        Row::from_values(vec![$($x),+])
    };
}

pub(crate) use row;

impl Row {
    fn new() -> Self {
        Row {
            columns: Vec::new(),
        }
    }

    pub fn from_values(values: Vec<Value>) -> Self {
        Row {
            columns: values.into_iter().map(Some).collect(),
        }
    }
}

impl Into<Vec<u8>> for Value {
    fn into(self) -> Vec<u8> {
        let type_ = self.column_type().type_identifier().to_be_bytes();

        let mut bytes = match self {
            Value::Ascii(v) => {
                let size = (v.len() as u32).to_be_bytes().to_vec();
                [size, v].concat()
            }
            Value::Bigint(i) => i.to_be_bytes().to_vec(),
            Value::Blob(v) => {
                let size = (v.len() as u32).to_be_bytes().to_vec();
                [size, v].concat()
            }
            Value::Boolean(b) => vec![b as u8],
            Value::Counter(i) => i.to_be_bytes().to_vec(),
            Value::Decimal(v) => {
                let size = (v.len() as u32).to_be_bytes().to_vec();
                [size, v].concat()
            }
            Value::Double(f) => f.to_be_bytes().to_vec(),
            Value::Float(f) => f.to_be_bytes().to_vec(),
            Value::Int(i) => i.to_be_bytes().to_vec(),
            Value::Timestamp(t) => t.to_be_bytes().to_vec(),
            Value::Uuid(u) => u.as_bytes().to_vec(),
            Value::Varchar(s) => {
                let bytes = s.into_bytes();
                let size = (bytes.len() as u32).to_be_bytes().to_vec();
                [size, bytes].concat()
            }
            Value::Varint(v) => {
                let size = (v.len() as u32).to_be_bytes().to_vec();
                [size, v].concat()
            }
            Value::Timeuuid(u) => u.as_bytes().to_vec(),
            Value::Inet(v) => {
                let size = (v.len() as u32).to_be_bytes().to_vec();
                [size, v].concat()
            }
            Value::Date(d) => d.to_be_bytes().to_vec(),
            Value::Time(t) => t.to_be_bytes().to_vec(),
            Value::Smallint(i) => i.to_be_bytes().to_vec(),
            Value::Tinyint(i) => i.to_be_bytes().to_vec(),
        };

        let mut result = type_.to_vec();

        result.append(&mut bytes);
        result
    }
}

impl PartialEq for Row {
    fn eq(&self, other: &Self) -> bool {
        self.columns == other.columns
    }
}

const NOT_NULL: u8 = 1;
const NULL: u8 = 0;

impl Into<Slice> for Row {
    fn into(self) -> Slice {
        let mut bytes = Vec::new();

        println!("Writing to slice: {:?}", self);

        for value in self.columns {
            match value {
                Some(v) => {
                    let value_bytes: Vec<u8> = v.into();

                    bytes.extend(vec![NOT_NULL]);
                    bytes.extend(value_bytes);
                }
                None => {
                    bytes.extend(vec![NULL]);
                }
            }
        }

        Slice::from(bytes)
    }
}

impl From<Slice> for Row {
    fn from(value: Slice) -> Self {
        let mut columns: Vec<Option<Value>> = Vec::new();
        let mut remaining = value.as_ref();

        while !remaining.is_empty() {
            let (present_bytes, rest) = remaining.split_at(1);

            if present_bytes[0] == NULL {
                columns.push(None);
                remaining = rest;

                continue;
            }

            let (type_bytes, rest) = rest.split_at(2);
            let type_id = u16::from_be_bytes(type_bytes.try_into().unwrap());

            let (column_bytes, rest) = match type_id {
                ASCII_TYPE_ID | BLOB_TYPE_ID | DECIMAL_TYPE_ID | VARCHAR_TYPE_ID
                | VARINT_TYPE_ID | INET_TYPE_ID => {
                    let (size_bytes, rest) = rest.split_at(4);
                    let size = u32::from_be_bytes(size_bytes.try_into().unwrap()) as usize;
                    println!("  Size: {}", size.to_string());
                    rest.split_at(size)
                }
                BIGINT_TYPE_ID => rest.split_at(8),
                BOOLEAN_TYPE_ID => rest.split_at(1),
                COUNTER_TYPE_ID => rest.split_at(8),
                DOUBLE_TYPE_ID => rest.split_at(8),
                FLOAT_TYPE_ID => rest.split_at(4),
                INT_TYPE_ID => rest.split_at(4),
                TIMESTAMP_TYPE_ID => rest.split_at(8),
                UUID_TYPE_ID => rest.split_at(16),
                TIMEUUID_TYPE_ID => rest.split_at(16),
                DATE_TYPE_ID => rest.split_at(4),
                TIME_TYPE_ID => rest.split_at(8),
                SMALLINT_TYPE_ID => rest.split_at(2),
                TINYINT_TYPE_ID => rest.split_at(1),
                _ => unimplemented!(),
            };

            let column = {
                match type_id {
                    ASCII_TYPE_ID => Value::Ascii(column_bytes.to_vec()),
                    BIGINT_TYPE_ID => {
                        Value::Bigint(i64::from_be_bytes(column_bytes.try_into().unwrap()))
                    }
                    BLOB_TYPE_ID => Value::Blob(column_bytes.to_vec()),
                    BOOLEAN_TYPE_ID => Value::Boolean(column_bytes[0] != 0),
                    COUNTER_TYPE_ID => {
                        Value::Counter(i64::from_be_bytes(column_bytes.try_into().unwrap()))
                    }
                    DECIMAL_TYPE_ID => Value::Decimal(column_bytes.to_vec()),
                    DOUBLE_TYPE_ID => {
                        Value::Double(f64::from_be_bytes(column_bytes.try_into().unwrap()))
                    }
                    FLOAT_TYPE_ID => {
                        Value::Float(f32::from_be_bytes(column_bytes.try_into().unwrap()))
                    }
                    INT_TYPE_ID => Value::Int(i32::from_be_bytes(column_bytes.try_into().unwrap())),
                    TIMESTAMP_TYPE_ID => {
                        Value::Timestamp(i64::from_be_bytes(column_bytes.try_into().unwrap()))
                    }
                    UUID_TYPE_ID => Value::Uuid(Uuid::from_slice(column_bytes).unwrap()),
                    VARCHAR_TYPE_ID => {
                        Value::Varchar(String::from_utf8(column_bytes.to_vec()).unwrap())
                    }
                    VARINT_TYPE_ID => Value::Varint(column_bytes.to_vec()),
                    TIMEUUID_TYPE_ID => Value::Timeuuid(Uuid::from_slice(column_bytes).unwrap()),
                    INET_TYPE_ID => Value::Inet(column_bytes.to_vec()),
                    DATE_TYPE_ID => {
                        Value::Date(i32::from_be_bytes(column_bytes.try_into().unwrap()))
                    }
                    TIME_TYPE_ID => {
                        Value::Time(i64::from_be_bytes(column_bytes.try_into().unwrap()))
                    }
                    SMALLINT_TYPE_ID => {
                        Value::Smallint(i16::from_be_bytes(column_bytes.try_into().unwrap()))
                    }
                    TINYINT_TYPE_ID => {
                        Value::Tinyint(i8::from_be_bytes(column_bytes.try_into().unwrap()))
                    }
                    _ => unimplemented!(),
                }
            };

            columns.push(Some(column));
            remaining = rest;
        }

        let row = Row { columns };

        println!("Reading from slice: {:?}", row);

        row
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fjall::Slice;
    use uuid::Uuid;

    #[test]
    fn test_value_column_type() {
        assert_eq!(
            Value::Ascii(vec![65, 66, 67]).column_type(),
            ColumnType::Ascii
        );
        assert_eq!(Value::Bigint(123456789).column_type(), ColumnType::Bigint);
        assert_eq!(Value::Blob(vec![1, 2, 3]).column_type(), ColumnType::Blob);
        assert_eq!(Value::Boolean(true).column_type(), ColumnType::Boolean);
        assert_eq!(Value::Counter(987654321).column_type(), ColumnType::Counter);
        assert_eq!(
            Value::Decimal(vec![1, 2, 3, 4]).column_type(),
            ColumnType::Decimal
        );
        assert_eq!(
            Value::Double(std::f64::consts::PI).column_type(),
            ColumnType::Double
        );
        assert_eq!(
            Value::Float(std::f32::consts::E).column_type(),
            ColumnType::Float
        );
        assert_eq!(Value::Int(42).column_type(), ColumnType::Int);
        assert_eq!(
            Value::Timestamp(1627846261).column_type(),
            ColumnType::Timestamp
        );
        assert_eq!(Value::Uuid(Uuid::new_v4()).column_type(), ColumnType::Uuid);
        assert_eq!(
            Value::Varchar("test".to_string()).column_type(),
            ColumnType::Varchar
        );
        assert_eq!(
            Value::Varint(vec![1, 2, 3, 4]).column_type(),
            ColumnType::Varint
        );
        assert_eq!(
            Value::Timeuuid(Uuid::new_v4()).column_type(),
            ColumnType::Timeuuid
        );
        assert_eq!(
            Value::Inet(vec![192, 168, 1, 1]).column_type(),
            ColumnType::Inet
        );
        assert_eq!(Value::Date(18628).column_type(), ColumnType::Date);
        assert_eq!(Value::Time(1234567890).column_type(), ColumnType::Time);
        assert_eq!(Value::Smallint(123).column_type(), ColumnType::Smallint);
        assert_eq!(Value::Tinyint(12).column_type(), ColumnType::Tinyint);
    }

    #[test]
    fn test_row_equality() {
        let row1 = Row::from_values(vec![Value::Ascii(b"Hello".to_vec()), Value::Int(42)]);
        let row2 = Row::from_values(vec![Value::Ascii(b"Hello".to_vec()), Value::Int(42)]);

        assert_eq!(row1, row2);
    }

    #[test]
    fn test_value_equality() {
        let value1 = Value::Int(42);
        let value2 = Value::Int(42);
        assert_eq!(value1, value2);
    }

    #[test]
    fn test_value_inequality() {
        let value1 = Value::Int(42);
        let value2 = Value::Int(43);
        assert_ne!(value1, value2);
    }

    #[test]
    fn test_row_inequality() {
        let row1 = Row::from_values(vec![Value::Ascii(b"Hello".to_vec()), Value::Int(42)]);
        let row2 = Row::from_values(vec![Value::Ascii(b"Hello".to_vec()), Value::Int(43)]);

        assert_ne!(row1, row2);
    }

    #[test]
    fn test_from_cql_type() {
        assert_eq!(
            ColumnType::from_cql_type("int".to_string()),
            Some(ColumnType::Int)
        );
        assert_eq!(
            ColumnType::from_cql_type("INT".to_string()),
            Some(ColumnType::Int)
        );
        assert_eq!(
            ColumnType::from_cql_type("text".to_string()),
            Some(ColumnType::Varchar)
        );
        assert_eq!(
            ColumnType::from_cql_type("varchar".to_string()),
            Some(ColumnType::Varchar)
        );
        assert_eq!(
            ColumnType::from_cql_type("uuid".to_string()),
            Some(ColumnType::Uuid)
        );
        assert_eq!(ColumnType::from_cql_type("invalid".to_string()), None);
    }

    #[test]
    fn test_slice_to_row() {
        let values = vec![
            Value::Ascii(vec![65, 66, 67]),
            Value::Bigint(123456789),
            Value::Blob(vec![1, 2, 3]),
            Value::Boolean(true),
            Value::Counter(987654321),
            Value::Decimal(vec![1, 2, 3, 4]),
            Value::Double(std::f64::consts::PI),
            Value::Float(std::f32::consts::E),
            Value::Int(42),
            Value::Timestamp(1627846261),
            Value::Uuid(Uuid::new_v4()),
            Value::Varchar("test".to_string()),
            Value::Varint(vec![1, 2, 3, 4]),
            Value::Timeuuid(Uuid::new_v4()),
            Value::Inet(vec![192, 168, 1, 1]),
            Value::Date(18628),
            Value::Time(1234567890),
            Value::Smallint(123),
            Value::Tinyint(12),
        ];

        let row = Row::from_values(values.clone());
        let slice: Slice = row.clone().into();

        println!("{:?}", slice);

        let row2: Row = slice.into();

        assert_eq!(row, row2);
    }

    #[test]
    fn test_slice_to_row_with_nulls() {
        let values = vec![
            Some(Value::Ascii(vec![65, 66, 67])),
            None,
            Some(Value::Bigint(123456789)),
            None,
            Some(Value::Boolean(true)),
            None,
            Some(Value::Int(42)),
        ];

        let row = Row {
            columns: values.clone(),
        };
        let slice: Slice = row.clone().into();

        let row2: Row = slice.into();

        assert_eq!(row, row2);
    }
}

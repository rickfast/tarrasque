use sled::IVec;
use std::fmt::Debug;
use uuid::Uuid;

#[derive(Debug, Copy, Clone)]
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
const VARCHAR: u16 = 0x000D;
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
            ColumnType::Varchar => VARINT_TYPE_ID,
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
enum Value {
    Ascii(Vec<u8>),
    Bigint(i64),
    Blob(Vec<u8>),
    Boolean(bool),
    Counter(),
    Decimal,
    Double(f64),
    Float(f32),
    Int(i32),
    Timestamp,
    Uuid(Uuid),
    Varchar(String),
    Varint,
    Timeuuid(Uuid),
    Inet,
    Date(f64),
    Time(f64),
    Smallint(i16),
    Tinyint(i8),
}

impl Value {
    fn column_type(&self) -> ColumnType {
        match self {
            Value::Ascii(_) => ColumnType::Ascii,
            Value::Bigint(_) => ColumnType::Bigint,
            Value::Blob(_) => ColumnType::Blob,
            Value::Boolean(_) => ColumnType::Boolean,
            Value::Counter() => ColumnType::Counter,
            Value::Decimal => ColumnType::Decimal,
            Value::Double(_) => ColumnType::Double,
            Value::Float(_) => ColumnType::Float,
            Value::Int(_) => ColumnType::Int,
            Value::Timestamp => ColumnType::Timestamp,
            Value::Uuid(_) => ColumnType::Uuid,
            Value::Varchar(_) => ColumnType::Varchar,
            Value::Varint => ColumnType::Varint,
            Value::Timeuuid(_) => ColumnType::Timeuuid,
            Value::Inet => ColumnType::Inet,
            Value::Date(_) => ColumnType::Date,
            Value::Time(_) => ColumnType::Time,
            Value::Smallint(_) => ColumnType::Smallint,
            Value::Tinyint(_) => ColumnType::Tinyint,
        }
    }
}

impl Into<IVec> for Value {
    fn into(self) -> IVec {
        let mut result = self.column_type().type_identifier().to_be_bytes().to_vec();

        let bytes = match self {
            Value::Smallint(value) => value.to_be_bytes(),
            _ => unreachable!(),
        };

        result.extend_from_slice(bytes.as_slice());

        IVec::from(result)
    }
}

impl From<IVec> for Value {
    fn from(v: IVec) -> Self {
        let (type_bytes, data_bytes) = v.split_at(2);
        let type_identifier = u16::from_be_bytes(type_bytes.try_into().unwrap());

        match type_identifier {
            SMALLINT_TYPE_ID => Value::Smallint(i16::from_be_bytes(data_bytes.try_into().unwrap())),
            _ => unimplemented!(),
        }
    }
}

impl PartialEq<Self> for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Smallint(i), Value::Smallint(j)) => i == j,
            _ => unimplemented!(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Row {
    columns: Vec<Value>,
}

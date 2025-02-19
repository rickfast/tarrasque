use sqlparser::dialect::Dialect;

#[derive(Debug, Clone)]
pub struct CassandraDialect {}

impl Dialect for CassandraDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        ch.is_alphabetic()
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        ch.is_alphabetic() || ch.is_numeric() || ch == '_'
    }
}

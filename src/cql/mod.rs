use crate::cql::operation::Operation;

pub mod codec;
pub mod header;
pub mod operation;
pub mod request;
pub mod response;

pub(crate) const CQL_VERSION_KEY: &str = "CQL_VERSION";
pub(crate) const CQL_VERSION_VALUE: &str = "3.0.0";

pub(crate) const PROTOCOL_VERSIONS_KEY: &str = "PROTOCOL_VERSIONS";
pub(crate) const PROTOCOL_VERSIONS_VALUE: &str = "4/v4";

use crate::cql::request::query::Query;
use std::collections::HashMap;

pub(crate) const ERROR_OP_CODE: u8 = 0x00;
pub(crate) const STARTUP_OP_CODE: u8 = 0x01;
pub(crate) const READY_OP_CODE: u8 = 0x02;
pub(crate) const AUTHENTICATE_OP_CODE: u8 = 0x03;
pub(crate) const OPTIONS_OP_CODE: u8 = 0x05;
pub(crate) const SUPPORTED_OP_CODE: u8 = 0x06;
pub(crate) const QUERY_OP_CODE: u8 = 0x07;
pub(crate) const RESULT_OP_CODE: u8 = 0x08;
pub(crate) const PREPARE_OP_CODE: u8 = 0x09;
pub(crate) const EXECUTE_OP_CODE: u8 = 0x0A;
pub(crate) const REGISTER_OP_CODE: u8 = 0x0B;
pub(crate) const EVENT_OP_CODE: u8 = 0x0C;
pub(crate) const BATCH_OP_CODE: u8 = 0x0D;
pub(crate) const AUTH_CHALLENGE_OP_CODE: u8 = 0x0E;
pub(crate) const AUTH_RESPONSE_OP_CODE: u8 = 0x0F;
pub(crate) const AUTH_SUCCESS_OP_CODE: u8 = 0x10;

#[derive(Debug, Clone)]
pub enum Operation {
    Error(crate::cql::response::error::Error),
    Startup(HashMap<String, String>),
    Ready,
    Authenticate,
    Options,
    Supported(HashMap<&'static str, &'static str>),
    Query(Query),
    Result(crate::cql::response::result::Result),
    Prepare,
    Execute,
    Register,
    Event,
    Batch,
    AuthChallenge,
    AuthResponse,
    AuthSuccess,
}

impl Operation {
    pub fn op_code(&self) -> u8 {
        match self {
            Operation::Error(_) => ERROR_OP_CODE,
            Operation::Startup(_) => STARTUP_OP_CODE,
            Operation::Ready => READY_OP_CODE,
            Operation::Authenticate => AUTHENTICATE_OP_CODE,
            Operation::Options => OPTIONS_OP_CODE,
            Operation::Supported(_) => SUPPORTED_OP_CODE,
            Operation::Query(_) => QUERY_OP_CODE,
            Operation::Result(_) => RESULT_OP_CODE,
            Operation::Prepare => PREPARE_OP_CODE,
            Operation::Execute => EXECUTE_OP_CODE,
            Operation::Register => REGISTER_OP_CODE,
            Operation::Event => EVENT_OP_CODE,
            Operation::Batch => BATCH_OP_CODE,
            Operation::AuthChallenge => AUTH_CHALLENGE_OP_CODE,
            Operation::AuthResponse => AUTH_RESPONSE_OP_CODE,
            Operation::AuthSuccess => AUTH_SUCCESS_OP_CODE,
        }
    }
}

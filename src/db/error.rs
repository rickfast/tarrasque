#[derive(Debug, Clone)]
pub struct DbError {
    pub code: ErrorCode,
    pub message: String,
}

impl DbError {
    pub fn new(code: ErrorCode, message: String) -> DbError {
        DbError { code, message }
    }

    // pub fn to_error(&self) -> Error {
    //     Error::new(self.code.to_code(), self.message.clone())
    // }
}

#[derive(Debug, Copy, Clone)]
pub enum ErrorCode {
    SyntaxError,
    Unauthorized,
    Invalid,
    ConfigError,
    AlreadyExists,
    Unprepared,
    ReadTimeout,
    WriteTimeout,
    ReadFailure,
    WriteFailure,
    FunctionFailure,
    ProtocolError,
    Overloaded,
    IsBootstrapping,
    TruncateError,
    ServerError,
    Unavailable,
}

impl ErrorCode {
    pub fn to_code(&self) -> i32 {
        match self {
            ErrorCode::SyntaxError => 0x2000,
            ErrorCode::Unauthorized => 0x2100,
            ErrorCode::Invalid => 0x2200,
            ErrorCode::ConfigError => 0x2300,
            ErrorCode::AlreadyExists => 0x2400,
            ErrorCode::Unprepared => 0x2500,
            ErrorCode::ReadTimeout => 0x1200,
            ErrorCode::WriteTimeout => 0x1300,
            ErrorCode::ReadFailure => 0x1400,
            ErrorCode::WriteFailure => 0x1500,
            ErrorCode::FunctionFailure => 0x1600,
            ErrorCode::ProtocolError => 0x000A,
            ErrorCode::Overloaded => 0x000B,
            ErrorCode::IsBootstrapping => 0x000C,
            ErrorCode::TruncateError => 0x000D,
            ErrorCode::ServerError => 0x0000,
            ErrorCode::Unavailable => 0x1000,
        }
    }
}
#![warn(rust_2018_idioms)]

use std::collections::HashMap;
use tokio::net::{TcpListener, TcpStream};

use crate::cql::codec::CqlFrameCodec;
use crate::cql::operation::Operation;
use crate::db::{Database, Results};
use crate::cql::response::error::Error as CqlError;
use crate::cql::response::result::{Flags, Metadata, Result as CqlResult};
use futures::sink::SinkExt;
use std::env;
use std::error::Error;
use tokio_stream::StreamExt;
use tokio_util::codec::Framed;
use crate::db::data::{Row, Value};
use crate::db::error::DbError;

const _256MB: usize = 26435456;

pub async fn main() -> Result<(), Box<dyn Error>> {
    let addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:9042".to_string());
    let conn = TcpListener::bind(addr).await?;

    match conn.accept().await {
        Ok((socket, _addr)) => {
            let mut server = Framed::new(socket, CqlFrameCodec::new());
            exchange(&mut server).await?;
        }
        Err(_) => {}
    }

    Ok(())
}

async fn exchange(server: &mut Framed<TcpStream, CqlFrameCodec>) -> Result<(), Box<dyn Error>> {
    let database = Database::new(HashMap::new());

    while let Some(result) = server.next().await {
        match result {
            Ok(frame) => match frame {
                Operation::Error(error) => {
                    unimplemented!()
                }
                Operation::Startup(_) => {
                    server.send(Operation::Ready).await?;
                }
                Operation::Ready => {}
                Operation::Authenticate => {}
                Operation::Options => {
                    server
                        .send(Operation::Supported(HashMap::from([
                            (crate::cql::CQL_VERSION_KEY, crate::cql::CQL_VERSION_VALUE),
                            (
                                crate::cql::PROTOCOL_VERSIONS_KEY,
                                crate::cql::PROTOCOL_VERSIONS_VALUE,
                            ),
                        ])))
                        .await?;
                }
                Operation::Query(query) => match database.clone().query(query) {
                    Ok(result) => {
                        let iterator = result.result;
                        let items = iterator.map(|row| {
                            Row {
                                columns: row.into_iter().collect::<Vec<Value>>(),
                            }
                        }).collect::<Vec<Row>>();

                        let result = CqlResult::Rows {
                            rows: items.clone(),
                            metadata: Metadata::new(Flags::empty(), 2),
                            row_count: items.len() as i32,
                        };

                        server.send(Operation::Result(result)).await?;
                    }
                    Err(error) => {
                        server.send(Operation::Error(error.into())).await?;
                    }
                },
                Operation::Prepare => {}
                Operation::Execute => {}
                Operation::Register => {}
                Operation::Event => {}
                Operation::Batch => {}
                Operation::AuthChallenge => {}
                Operation::AuthResponse => {}
                Operation::AuthSuccess => {}
                _ => println!("Operation {:?} is not a Request type", frame),
            },
            Err(e) => println!("Error while reading frame: {}", e),
        }
    }

    Ok(())
}

impl Into<CqlError> for DbError {
    fn into(self) -> CqlError {
        CqlError::new(self.code.to_code(), self.message)
    }
}
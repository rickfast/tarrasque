extern crate core;

mod cql;
mod db;
mod serde;
mod server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    server::cql::main().await
}

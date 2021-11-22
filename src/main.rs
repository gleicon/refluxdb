use std::error::Error;
use std::net::SocketAddr;
use std::{env, io};
use tokio::net::UdpSocket;

// cargo run
// nc -u 127.0.0.1 8089
mod persistence;
mod protocol;
mod udpserver;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:8089".to_string());
    let pm = persistence::TimeseriesDiskPersistenceManager::new("databases".to_string());
    let socket = UdpSocket::bind(&addr).await?;
    println!("Listening on: {}", socket.local_addr()?);

    let server = udpserver::UDPRefluxServer::new(addr);

    // This starts the server task.
    let mut srv = server.await;
    srv.run().await?;

    Ok(())
}

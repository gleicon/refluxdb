use std::error::Error;
use std::net::SocketAddr;
use std::{env, io};
use tokio::net::UdpSocket;

// cargo run
// nc -u 127.0.0.1 8089
mod persistence;
mod protocol;

struct Server {
    socket: UdpSocket,
    buf: Vec<u8>,
    to_send: Option<(usize, SocketAddr)>,
}

impl Server {
    async fn run(self) -> Result<(), io::Error> {
        let Server {
            socket,
            mut buf,
            mut to_send,
        } = self;

        loop {
            if let Some((size, peer)) = to_send {
                match protocol::LineProtocol::parse(
                    String::from_utf8_lossy(&buf[..size - 1]).to_string(),
                ) {
                    Ok(b) => {
                        let amt = socket
                            .send_to(b.serialize().unwrap().as_bytes(), &peer)
                            .await?;
                        println!(
                            "Echoed {}/{} bytes to {} - {:?}",
                            amt,
                            size,
                            peer,
                            String::from_utf8_lossy(&buf[..size - 1])
                        );
                    }
                    Err(e) => {
                        let amt = socket.send_to(e.as_bytes(), &peer).await?;
                        println!(
                            "Error: {}/{} bytes to {} - {}",
                            amt,
                            size,
                            peer,
                            e.to_string()
                        );
                    }
                }
            }

            to_send = Some(socket.recv_from(&mut buf).await?);
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:8089".to_string());

    let socket = UdpSocket::bind(&addr).await?;
    println!("Listening on: {}", socket.local_addr()?);

    let server = Server {
        socket,
        buf: vec![0; 1024],
        to_send: None,
    };

    // This starts the server task.
    server.run().await?;

    Ok(())
}

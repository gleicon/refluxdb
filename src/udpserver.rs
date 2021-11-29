use log::info;
use std::io;
use std::net::SocketAddr;
use tokio::net::UdpSocket;

pub struct UDPRefluxServer {
    pub socket: UdpSocket,
    buf: Vec<u8>,
    to_send: Option<(usize, SocketAddr)>,
    pm: crate::persistence::TimeseriesDiskPersistenceManager,
}

impl UDPRefluxServer {
    pub async fn run(&mut self) -> Result<(), io::Error> {
        loop {
            if let Some((size, peer)) = self.to_send {
                match crate::protocol::LineProtocol::parse(
                    String::from_utf8_lossy(&self.buf[..size - 1]).to_string(),
                ) {
                    Ok(b) => {
                        let amt = self
                            .socket
                            .send_to(b.serialize().unwrap().as_bytes(), &peer)
                            .await?;
                        println!(
                            "Echoed {}/{} bytes to {} - {:?}",
                            amt,
                            size,
                            peer,
                            String::from_utf8_lossy(&self.buf[..size - 1])
                        );
                    }
                    Err(e) => {
                        let amt = self.socket.send_to(e.as_bytes(), &peer).await?;
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

            self.to_send = Some(self.socket.recv_from(&mut self.buf).await?);
        }
    }
    pub async fn new(
        addr: String,
        pm: crate::persistence::TimeseriesDiskPersistenceManager,
    ) -> Self {
        let socket = UdpSocket::bind(&addr).await.unwrap();
        info!("Listening on UDP: {}", socket.local_addr().unwrap());

        let s = Self {
            socket,
            buf: vec![0; 1024],
            to_send: None,
            pm: pm,
        };
        return s;
    }
}

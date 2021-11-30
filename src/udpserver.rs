use log::{debug, info};
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::net::UdpSocket;

pub struct UDPRefluxServer {
    pub socket: UdpSocket,
    buf: Vec<u8>,
    to_send: Option<(usize, SocketAddr)>,
    pm: Arc<Mutex<crate::persistence::TimeseriesDiskPersistenceManager>>,
}

impl UDPRefluxServer {
    pub async fn run(&mut self) -> Result<(), io::Error> {
        loop {
            if let Some((size, peer)) = self.to_send {
                println!("--> {:?}", peer);

                match crate::protocol::LineProtocol::parse(
                    String::from_utf8_lossy(&self.buf[..size - 1]).to_string(),
                ) {
                    Ok(b) => {
                        let mut htags: HashMap<String, String> = HashMap::new();
                        for key in b.tag_set.clone().keys() {
                            htags.insert(key.into(), b.tag_set.get(key).unwrap().into());
                        }
                        // One line for each measurement, represented bu field_set
                        for field in b.field_set.clone() {
                            self.pm
                                .lock()
                                .unwrap()
                                .save_measurement(
                                    b.measurement_name.clone(),
                                    field.0,
                                    field.1,
                                    htags.clone(),
                                )
                                .unwrap();
                        }
                        // echo back code
                        // let amt = self
                        //     .socket
                        //     .send_to(b.serialize().unwrap().as_bytes(), &peer)
                        //     .await?;
                        debug!(
                            "got {:?}/{} bytes to {} - {:?}",
                            b.serialize().clone(),
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
        pm: Arc<Mutex<crate::persistence::TimeseriesDiskPersistenceManager>>,
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

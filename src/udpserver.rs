use actix_rt::net::UdpSocket;
use log::{debug, info};
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

pub struct UDPRefluxServer {
    pub socket: UdpSocket,
    buf: Vec<u8>,
    to_send: Option<(usize, SocketAddr)>,
    pm: Arc<Mutex<crate::persistence::TimeseriesDiskPersistenceManager>>,
}

impl UDPRefluxServer {
    pub async fn run(&mut self, echo: bool) -> Result<(), io::Error> {
        info!("Processing UDP metrics");

        self.to_send = Some(self.socket.recv_from(&mut self.buf).await?);
        loop {
            debug!("--> {:?}", self.to_send);

            if let Some((size, peer)) = self.to_send {
                match crate::protocol::LineProtocol::parse(
                    String::from_utf8_lossy(&self.buf[..size - 1]).to_string(),
                ) {
                    Ok(b) => {
                        let mut htags: HashMap<String, String> = HashMap::new();
                        for key in b.tag_set.clone().keys() {
                            htags.insert(key.into(), b.tag_set.get(key).unwrap().into());
                        }
                        // One line for each measurement, represented b field_set
                        for field in b.field_set.clone() {
                            match self.pm.lock().unwrap().save_measurement(
                                b.measurement_name.clone(),
                                field.0.clone(),
                                field.1.clone(),
                                htags.clone(),
                            ) {
                                Ok(_) => info!(
                                    "Timeseries {} Measurement {} value {}",
                                    b.measurement_name.clone(),
                                    field.0.clone(),
                                    field.1.clone()
                                ),
                                Err(e) => info!("Error writing measurement: {}", e),
                            };
                        }
                        //echo back the line
                        let bs = b.serialize().clone();
                        if echo {
                            let _ = self
                                .socket
                                .send_to(bs.clone().unwrap().as_bytes(), &peer)
                                .await?;
                        }
                        info!(
                            "got {:?}/{} bytes to {} - {:?}",
                            bs.clone().unwrap(),
                            size,
                            peer,
                            String::from_utf8_lossy(&self.buf[..size - 1])
                        );
                    }
                    Err(e) => {
                        // echoes error back
                        if echo {
                            let amt = self.socket.send_to(e.as_bytes(), &peer).await?;
                            info!(
                                "Error: {}/{} bytes to {} - {}",
                                amt,
                                size,
                                peer,
                                e.to_string()
                            );
                        } else {
                            info!(
                                "Error: parsing {} bytes to {} - {}",
                                size,
                                peer,
                                e.to_string()
                            );
                        }
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

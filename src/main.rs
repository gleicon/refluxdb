use std::error::Error;
use std::net::SocketAddr;
use std::{env, io};
use tokio::net::UdpSocket;

// cargo run
// nc -u 127.0.0.1 8089
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
                let b = protocol::LineProtocol::parse(String::from_utf8_lossy(&buf).to_string());
                // match b {
                //     Ok(p) => print!("{:#?}\n", p.serialize().unwrap()),
                //     Err(e) => print!("Error: {:?}", e)
                // }

//                let amt = socket.send_to(&buf[..size], &peer).await?;
                let amt = socket.send_to(b.unwrap().serialize().unwrap().as_bytes(), &peer).await?;
                
                    
                println!("Echoed {}/{} bytes to {} - {:?}", amt, size, peer, String::from_utf8_lossy(&buf[..size-1]));
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

//     let res = protocol::LineProtocol::parse("myMeasurement,tag1=value1,tag2=value2 fieldKey=\"fieldValue\" 1556813561098000000".to_string());
//     let res2 = protocol::LineProtocol::parse("myMeasurement2 fieldKey=\"fieldValue\" 1556813561098000000".to_string());

//     match res {
//          Ok(p) => print!("{:#?}\n", p.serialize().unwrap()),
//          Err(e) => print!("Error: {:?}", e)
//     }

//     match res2 {
//         Ok(p) => print!("{:#?}\n", p.serialize().unwrap()),
//         Err(e) => print!("Error: {:?}", e)
//    }
    
    let server = Server {
        socket,
        buf: vec![0; 1024],
        to_send: None,
    };

    // This starts the server task.
    server.run().await?;

    Ok(())
}

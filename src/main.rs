use std::convert::Infallible;
use std::env;
use std::error::Error;
use tokio::net::UdpSocket;
use tokio::task;

use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server};

// cargo run
// nc -u 127.0.0.1 8089
mod persistence;
mod protocol;
mod udpserver;

async fn hello(_: Request<Body>) -> Result<Response<Body>, Infallible> {
    Ok(Response::new(Body::from("Hello World!")))
}

// TODO: http query interface + UDP write interface
// TODO GlueSQL query engine
// TODO: use hyper for http in the main thread, udp in a service thread.

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    pretty_env_logger::init();

    let addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:8089".to_string());
    let pm = persistence::TimeseriesDiskPersistenceManager::new("databases".to_string());

    // spawns and wait for the UDPServer
    tokio::spawn(async {
        let server = udpserver::UDPRefluxServer::new(addr);
        let mut srv = server.await;
        srv.run().await.unwrap();
    });

    let make_svc = make_service_fn(|_conn| async { Ok::<_, Infallible>(service_fn(hello)) });
    let addr = ([127, 0, 0, 1], 8081).into();
    let server = Server::bind(&addr).serve(make_svc);

    println!("Listening on http://{}", addr);

    server.await?;

    Ok(())
}

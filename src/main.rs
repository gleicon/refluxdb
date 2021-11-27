use actix_web::{middleware, web, App, HttpServer};
use log::info;
use std::env;
use std::sync::{Arc, Mutex};

// cargo run
// nc -u 127.0.0.1 8089
mod handlers;
mod persistence;
mod protocol;
mod udpserver;

// TODO: http query interface + UDP write interface
// TODO GlueSQL query engine
// TODO: use hyper for http in the main thread, udp in a service thread.
#[actix_web::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var(
        "RUST_LOG",
        "actix_web=debug,actix_server=debug,refluxdb=info",
    );
    env_logger::init();

    let addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:8089".to_string());
    let pm = persistence::TimeseriesDiskPersistenceManager::new("databases".to_string());
    let data = web::Data::new(Arc::new(Mutex::new(pm)));

    let pmc = pm; //.clone();
                  // spawns and wait for the UDPServer
    let _task = actix::spawn(async {
        let server = udpserver::UDPRefluxServer::new(addr, pmc);
        let mut srv = server.await;
        srv.run().await.unwrap();
    });

    info!("Listening to http");
    HttpServer::new(move || {
        App::new()
            .wrap(middleware::Logger::default())
            .app_data(data.clone())
            .service(handlers::write_timeseries)
            .service(handlers::query_timeseries)
            .service(handlers::list_timeseries)
    })
    .bind("127.0.0.1:8086")?
    .run()
    .await
}

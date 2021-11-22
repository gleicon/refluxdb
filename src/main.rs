use actix::spawn;
use actix_web::{middleware, web, App, HttpServer};
use std::env;

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
    pretty_env_logger::init();

    let addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:8089".to_string());
    let pm = persistence::TimeseriesDiskPersistenceManager::new("databases".to_string());

    // spawns and wait for the UDPServer
    let _task = actix::spawn(async {
        print!("Listening to udp\n");
        let server = udpserver::UDPRefluxServer::new(addr);
        let mut srv = server.await;
        srv.run().await.unwrap();
        print!("Listening to udp\n");
    });

    print!("Listening to http\n");
    HttpServer::new(move || {
        App::new()
            .wrap(middleware::Logger::default())
            //            .app_data(data.clone())
            .service(handlers::write_timeseries)
            .service(handlers::query_timeseries)
            .service(handlers::list_timeseries)
    })
    .bind("127.0.0.1:8081")?
    .run()
    .await
}

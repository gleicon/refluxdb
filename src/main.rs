use actix_web::{middleware, web, App, HttpServer};
use log::info;
use std::sync::{Arc, Mutex};

// cargo run
// echo "hi"| nc -u 127.0.0.1 8089
mod handlers;
mod persistence;
mod protocol;
mod udpserver;

#[actix_rt::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var(
        "RUST_LOG",
        "actix_web=info,actix_server=info,refluxdb=info,refluxdb::handlers=info",
    );
    env_logger::init();
    let db_dir = "databases";

    let addr = "127.0.0.1:8089".to_string();
    let pm = Arc::new(Mutex::new(
        persistence::TimeseriesDiskPersistenceManager::new(db_dir.to_string()),
    ));
    let data = web::Data::new(pm.clone());

    let _task = actix_rt::spawn(async move {
        let server = udpserver::UDPRefluxServer::new(addr, pm.clone());
        let mut srv = server.await;
        srv.run(false).await.unwrap(); // no echo back
    });

    info!("Listening to http");
    HttpServer::new(move || {
        App::new()
            .wrap(middleware::Logger::default())
            .app_data(data.clone())
            .service(handlers::write_timeseries)
            .service(handlers::query_timeseries)
            .service(handlers::list_timeseries)
            .service(handlers::query_timeseries_range)
    })
    .bind("127.0.0.1:8086")?
    .run()
    .await
}

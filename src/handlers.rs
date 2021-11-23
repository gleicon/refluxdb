use actix_web::{get, post, web, Error, HttpRequest, HttpResponse, Result};
use log::info;
use std::sync::Mutex;

#[get("/")]
async fn list_timeseries(
    pm: web::Data<crate::persistence::TimeseriesDiskPersistenceManager>,
) -> Result<HttpResponse, Error> {
    return Ok(HttpResponse::Ok()
        .content_type("application/json")
        .body("all ts"));
}

#[post("/query")]
async fn query_timeseries(
    req: HttpRequest,
    data: web::Data<Mutex<crate::persistence::TimeseriesDiskPersistenceManager>>,
) -> Result<HttpResponse, Error> {
    // q -> query string
    let qs = req.query_string();
    info!("{}", format!("{:?}", qs));
    let mut pm = data.lock().unwrap().clone();
    let pme = pm.pop_newest_measurement("teste".to_string());
    match pme {
        Ok(ret) => {
            return Ok(HttpResponse::Ok()
                .content_type("application/json")
                .json(format!("{:?}", ret)));
        }
        Err(e) => {
            return Ok(HttpResponse::BadRequest()
                .content_type("application/json")
                .body(format!("Query timeseries error: {}", e)));
        }
    }
}

/*
 * curl -i -XPOST 'http://localhost:8086/api/v2/write?bucket=db/rp&precision=ns' \
  --header 'Authorization: Token username:password' \
  --data-raw 'cpu_load_short,host=server01,region=us-west value=0.64 1434055562000000000'
*/
#[post("/write")]
async fn write_timeseries(
    req_body: String,
    pm: web::Data<Mutex<crate::persistence::TimeseriesDiskPersistenceManager>>,
) -> Result<HttpResponse, Error> {
    match crate::protocol::LineProtocol::parse(req_body.clone()) {
        Ok(b) => {
            return Ok(HttpResponse::Ok()
                .content_type("application/json")
                .json(format!("{:?}", b)));
        }
        Err(e) => {
            return Ok(HttpResponse::BadRequest()
                .content_type("application/json")
                .body(format!("{}", e)));
        }
    }
}

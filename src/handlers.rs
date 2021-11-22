use actix_web::{get, post, web, Error, HttpResponse, Result};
use serde::Deserialize;
use std::sync::Mutex;

#[derive(Deserialize, Clone)]
struct QueueInfo {
    queuename: String,
}

#[get("/")]
async fn list_timeseries() -> Result<HttpResponse, Error> {
    return Ok(HttpResponse::Ok()
        .content_type("application/json")
        .body("all ts"));
}

#[post("/query")]
async fn query_timeseries(info: web::Path<QueueInfo>) -> Result<HttpResponse, Error> {
    return Ok(HttpResponse::BadRequest()
        .content_type("application/json")
        .body(format!("Query timeseries")));
}

#[post("/write")]
async fn write_timeseries(info: web::Path<QueueInfo>) -> Result<HttpResponse, Error> {
    return Ok(HttpResponse::BadRequest()
        .content_type("application/json")
        .body(format!("Write timeseries")));
}

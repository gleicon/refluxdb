use actix_web::{get, post, web, Error, HttpResponse, Result};

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
    pm: web::Data<crate::persistence::TimeseriesDiskPersistenceManager>,
) -> Result<HttpResponse, Error> {
    return Ok(HttpResponse::BadRequest()
        .content_type("application/json")
        .body(format!("Query timeseries")));
}

#[post("/write")]
async fn write_timeseries(
    pm: web::Data<crate::persistence::TimeseriesDiskPersistenceManager>,
) -> Result<HttpResponse, Error> {
    return Ok(HttpResponse::BadRequest()
        .content_type("application/json")
        .body(format!("Write timeseries")));
}

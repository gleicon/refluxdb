use actix_web::{get, post, web, Error, HttpResponse, Result};
use chrono::{DateTime, Local, Utc};
use log::info;
use serde::Deserialize;
use std::sync::{Arc, Mutex};

#[derive(Deserialize, Clone)]
struct TimeseriesInfo {
    timeseries: String,
}

#[derive(Deserialize, Clone)]
pub struct RangeQueryRequest {
    start: String,
    end: String,
}

#[derive(Deserialize)]
struct FormData {
    q: String, // query string
}

#[get("/")]
async fn list_timeseries(
    pm: web::Data<Arc<Mutex<crate::persistence::TimeseriesDiskPersistenceManager>>>,
) -> Result<HttpResponse, Error> {
    let res = pm.lock().unwrap().clone().list_timeseries().unwrap();
    return Ok(HttpResponse::Ok()
        .content_type("application/json")
        .json(format!("{:?}", res)));
}

#[get("/range/{timeseries}")]
async fn query_timeseries_range(
    web::Query(info): web::Query<RangeQueryRequest>, // ?start=time&end=time
    ts: web::Path<TimeseriesInfo>,
    data: web::Data<Arc<Mutex<crate::persistence::TimeseriesDiskPersistenceManager>>>,
) -> Result<HttpResponse, Error> {
    // sanitize query and range strings
    // filter for existing ts only
    // no SQL query in this route
    let st = info.start.parse::<DateTime<Utc>>().unwrap();
    let en = info.end.parse::<DateTime<Utc>>().unwrap();
    let mut pm = data.lock().unwrap().clone();
    if !pm.clone().timeseries_exists(ts.timeseries.clone()) {
        return Ok(HttpResponse::NotFound()
            .content_type("application/json")
            .body(format!("Timeseries not found: {}", ts.timeseries.clone())));
    }
    let measurement_range = pm.get_measurement_range(
        ts.timeseries.clone(),
        st.timestamp_millis(),
        en.timestamp_millis(),
    );
    match measurement_range {
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

// consider this extremely insecure until proper SQL parsing and sanitization is implemented
// along with read only storage.
// The timeseries is contained into the query and should be validated before going down the db sink
#[post("/query")]
async fn query_timeseries(
    form: web::Form<FormData>,
    data: web::Data<Arc<Mutex<crate::persistence::TimeseriesDiskPersistenceManager>>>,
) -> Result<HttpResponse, Error> {
    // q -> query string
    let qs = form.q.clone();
    info!("{}", format!("{:?}", qs));
    let mut pm = data.lock().unwrap().clone();
    let pme = pm.query_measurements(qs.to_string());
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
  --data-raw 'cpu_load,host=server,region=us-east1 value=0.80 1234567890000000000'
*/
#[post("/write")]
async fn write_timeseries(
    req_body: String,
    pm: web::Data<Arc<Mutex<crate::persistence::TimeseriesDiskPersistenceManager>>>,
) -> Result<HttpResponse, Error> {
    match crate::protocol::LineProtocol::parse(req_body.clone()) {
        Ok(b) => {
            // persist
            return Ok(HttpResponse::Ok()
                .content_type("application/json")
                .json(format!("{:?}", b)));
        }
        Err(e) => {
            return Ok(HttpResponse::BadRequest()
                .content_type("application/json")
                .json(format!("{}", e)));
        }
    }
}

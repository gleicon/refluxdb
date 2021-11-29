use actix_web::{get, post, web, Error, HttpRequest, HttpResponse, Result};
use log::info;
use serde::Deserialize;
use std::sync::{Arc, Mutex};

#[derive(Deserialize, Clone)]
struct TimeseriesInfo {
    timeseries: String,
}

#[derive(Deserialize)]
pub struct RangeQueryRequest {
    start: i64,
    end: i64,
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
    req: HttpRequest,
    data: web::Data<Arc<Mutex<crate::persistence::TimeseriesDiskPersistenceManager>>>,
) -> Result<HttpResponse, Error> {
    // q -> query string
    info!("{}", format!("{} to {}", info.start, info.end));
    info!("{}", format!("{:?}", req));
    let mut pm = data.lock().unwrap().clone();
    let measurement_range = pm.get_measurement_range(ts.timeseries.clone(), info.start, info.end);
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

// #[post("/query")]
// async fn query_timeseries(
//     req: HttpRequest,
//     data: web::Data<Arc<Mutex<crate::persistence::TimeseriesDiskPersistenceManager>>>,
// ) -> Result<HttpResponse, Error> {
//     // q -> query string
//     let qs = req.query_string();
//     info!("{}", format!("{:?}", qs));
//     let mut pm = data.lock().unwrap().clone();
//     let pme = pm.pop_newest_measurement("teste".to_string());
//     match pme {
//         Ok(ret) => {
//             return Ok(HttpResponse::Ok()
//                 .content_type("application/json")
//                 .json(format!("{:?}", ret)));
//         }
//         Err(e) => {
//             return Ok(HttpResponse::BadRequest()
//                 .content_type("application/json")
//                 .body(format!("Query timeseries error: {}", e)));
//         }
//     }
// }

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

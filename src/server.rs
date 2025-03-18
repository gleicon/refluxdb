use actix_web::{web, App, HttpResponse, HttpServer, Responder};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::persistence::{Point, TimeseriesPersistenceManager};
use crate::sql::{parse_query, QueryError, QueryPlan};

#[derive(Debug, Serialize, Deserialize)]
pub struct QueryRequest {
    q: String,
}

#[derive(Debug, Serialize)]
pub struct QueryResponse {
    results: Vec<QueryResult>,
}

#[derive(Debug, Serialize)]
pub struct QueryResult {
    series: Vec<Series>,
}

#[derive(Debug, Serialize)]
pub struct Series {
    name: String,
    columns: Vec<String>,
    values: Vec<Vec<serde_json::Value>>,
}

pub struct Server {
    db: Arc<Mutex<TimeseriesPersistenceManager>>,
}

impl Server {
    pub fn new(db: TimeseriesPersistenceManager) -> Self {
        Self {
            db: Arc::new(Mutex::new(db)),
        }
    }

    pub async fn run(&self, addr: &str) -> std::io::Result<()> {
        let db = self.db.clone();
        HttpServer::new(move || {
            App::new()
                .app_data(web::Data::new(db.clone()))
                .route("/query", web::post().to(handle_query))
        })
        .bind(addr)?
        .run()
        .await
    }
}

async fn handle_query(
    req: web::Json<QueryRequest>,
    db: web::Data<Arc<Mutex<TimeseriesPersistenceManager>>>,
) -> impl Responder {
    match parse_query(&req.q) {
        Ok(plan) => {
            let db = db.lock().await;
            match db.query(&plan).await {
                Ok(points) => {
                    let results = points
                        .into_iter()
                        .map(|point| {
                            let mut columns = vec!["time".to_string()];
                            columns.extend(point.fields.keys().cloned());
                            let mut values = vec![serde_json::Value::Number(
                                serde_json::Number::from(point.timestamp.timestamp()),
                            )];
                            values.extend(point.fields.values().map(|v| {
                                serde_json::Value::Number(serde_json::Number::from_f64(*v).unwrap())
                            }));
                            Series {
                                name: point.measurement,
                                columns,
                                values: vec![values],
                            }
                        })
                        .collect();
                    HttpResponse::Ok().json(QueryResponse {
                        results: vec![QueryResult { series: results }],
                    })
                }
                Err(e) => HttpResponse::InternalServerError().json(format!("Query error: {}", e)),
            }
        }
        Err(e) => HttpResponse::BadRequest().json(format!("Parse error: {}", e)),
    }
}

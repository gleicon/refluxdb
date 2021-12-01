use gluesql::executor::FetchError;
use gluesql::prelude::*;

use log::info;
use std::collections::HashMap;
use uuid::Uuid;

pub fn parse_select_payload(payload: Payload) -> Result<crate::persistence::Measurement, String> {
    let rows = match payload {
        Payload::Select { labels: _, rows } => rows,
        _ => return Err(format!("Unexpected result: {:?}", payload)),
    };
    if rows.len() == 0 {
        return Err(format!("No data found for query"));
    };
    parse_select_resultset_row(&rows[0])
}

pub fn parse_select_payload_range(
    payload: &Payload,
) -> Result<Vec<crate::persistence::Measurement>, String> {
    let rows = match payload {
        Payload::Select { labels: _, rows } => rows,
        _ => return Err(format!("Unexpected result: {:?}", payload)),
    };
    let ev: Vec<crate::persistence::Measurement> = Vec::new();

    if rows.len() == 0 {
        //return Err(format!("No data found for query"));
        return Ok(ev);
    };
    let mut ev: Vec<crate::persistence::Measurement> = Vec::new();

    for row in rows {
        match parse_select_resultset_row(row) {
            Ok(e) => ev.push(e),
            Err(e) => return Err(format!("Error parsing range: {}", e)),
        }
    }
    Ok(ev)
}

pub fn parse_select_resultset_row(
    row: &std::vec::Vec<gluesql::data::Value>,
) -> Result<crate::persistence::Measurement, String> {
    // "CREATE TABLE {} (id UUID, time TIMESTAMP, created_at TIMESTAMP, name TEXT, value FLOAT, tags MAP);",
    let id = match &row[0] {
        Value::Uuid(i) => i,
        val => return Err(format!("Unexpected uuid value: {:?}", val)),
    };
    let key = match &row[1] {
        Value::Timestamp(key) => key,
        val => return Err(format!("Unexpected timestamp value: {:?}", val)),
    };
    let value = match &row[4] {
        Value::F64(value) => value,
        val => return Err(format!("Unexpected value: {:?}", val)),
    };
    let _ = match &row[5] {
        Value::Map(tags) => tags,
        //   _ => HashMap::new(), // TODO: temp mock
        val => return Err(format!("Unexpected tag value: {:?}", val)),
    };
    Ok(crate::persistence::Measurement {
        key: key.timestamp_millis().clone(),
        id: Uuid::from_u128(id.clone()),
        value: value.clone(),
        tags: HashMap::new(), //tt,
    })
}

pub fn check_db_schema(
    timeseries_name: String,
    storage: gluesql::storages::SledStorage,
    create: bool,
) -> Result<String, String> {
    let mut db = Glue::new(storage.clone());
    let query = format!("SELECT id from {} LIMIT 1;", timeseries_name,);
    match db.execute(&query) {
        Err(e) => match e {
            gluesql::result::Error::Fetch(FetchError::TableNotFound(a)) => {
                if !create {
                    return Err(format!("table fetch not found {:?} -> {}", a, query));
                } else {
                    // "CREATE TABLE <timeseries_name> (id UUID, time TIMESTAMP, created_at TIMESTAMP, name TEXT, value FLOAT, tags MAP);",
                    let query_create = format!(
                        "CREATE TABLE {} (id UUID, time TIMESTAMP, created_at TIMESTAMP, name TEXT, value FLOAT, tags MAP);",
                        timeseries_name
                    );
                    match db.execute(&query_create) {
                        Err(ei) => {
                            return Err(format!(
                                "Error creating table: {} - {} = {}",
                                timeseries_name, ei, query_create
                            ));
                        }
                        Ok(a) => {
                            info!("{:?}", a);
                            return Ok(format!(
                                "Database {} created: {:?}",
                                timeseries_name.clone(),
                                a
                            ));
                        }
                    };
                }
            }
            _ => {
                return Err(format!("query error: {} - {:?}", query, e));
            }
        },
        Ok(a) => {
            return Ok(format!(
                "database: {} check result {:?}",
                timeseries_name.clone(),
                a
            ))
        }
    };
}

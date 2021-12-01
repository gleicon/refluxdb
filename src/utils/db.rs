use gluesql::executor::FetchError;
use gluesql::prelude::*;

use log::info;
use std::collections::HashMap;
use uuid::Uuid;

pub fn parse_select_payload(
    payload: Payload,
) -> Result<Vec<crate::persistence::Measurement>, String> {
    let rows = match payload {
        Payload::Select { labels: _, rows } => rows,
        _ => return Err(format!("Unexpected result: {:?}", payload)),
    };
    if rows.len() == 0 {
        return Err(format!("No data found for query"));
    };
    let mut ev: Vec<crate::persistence::Measurement> = Vec::new();
    for row in rows {
        match parse_select_resultset_row(&row) {
            Ok(es) => {
                ev.push(es);
            }
            Err(e) => return Err(format!("Error parsing data: {}", e)),
        };
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
        val => return Err(format!("Unexpected tag value: {:?}", val)),
    };
    Ok(crate::persistence::Measurement {
        key: key.timestamp_millis().clone(),
        id: Uuid::from_u128(id.clone()),
        value: value.clone(),
        tags: HashMap::new(), //tt,
    })
}
/*
Check if a sled storage exists and if the timeseries schema is created. Optionally create it if create is set to true.
 */
pub fn check_or_create_database(
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

pub fn query_statement_tablename(query: String) -> Result<String, String> {
    match gluesql::parse_sql::parse(&query) {
        Ok(t) => match &t[0] {
            gluesql::sqlparser::ast::Statement::Query(tt) => match &tt.body {
                gluesql::sqlparser::ast::SetExpr::Select(ss) => match &ss.from[0].relation {
                    gluesql::sqlparser::ast::TableFactor::Table {
                        name,
                        alias: _,
                        args: _,
                        with_hints: _,
                    } => {
                        let tablename = &name.0[0].value;
                        Ok(tablename.clone())
                    }
                    _ => return Err(format!("No table found")),
                },
                _ => return Err(format!("Invalid SELECT statement: {}", tt.body)),
            },
            _ => return Err(format!("Unknown query: {}", t[0])),
        },
        Err(e) => return Err(format!("Improper query: {}", e)),
    }
}

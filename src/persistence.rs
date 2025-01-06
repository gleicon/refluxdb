use chrono::Local;
extern crate rusqlite;
use rusqlite::{params, Connection, Result};

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

// timeseries persistence manager
// Measurement: Unit of data tied to a time, within a timeseries, annotated with tags
// Timeseries: A set of immutable measurements that move forward in time
// Resultset: a slice of data from a timeseries within T(start) and T(end)
// Tags: json annotations to measurements used to filter and group resultsets
//
// Database structure:
// schema:
//      id -> UUID
//      time -> unix timestamp, ordered - measurement time (expanded 11-30-2021)
//      created_at -> unix timestamp, ordered, system time (added 11-30-2021)
//      name -> value name (added 11-30-2021)
//      value -> float (to be float, int, string and bool)
//      tags -> key/value tag map

//
// "CREATE TABLE <timeseries_name> (id UUID, time TIMESTAMP, created_at TIMESTAMP, name TEXT, value FLOAT, tags MAP);",
//
// TODO: separated tag table: "CREATE TABLE <timeseries_name>_tags (id UUID, key TEXT, value TEXT);",
// TODO: ensure immutability is enforced through measurement id or fingerprint
// TODO: Pre-calculated stats for each series

#[derive(Clone)]
pub struct TimeseriesPersistenceManager {
    pub connection: Arc<Mutex<Connection>>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Measurement {
    pub id: Uuid,  // Unique ID for each measurement
    pub time: i64, // Unix timestamp used as key
    pub created_at: i64,
    pub name: String,
    pub value: f64,
    pub tags: HashMap<String, String>,
}

impl TimeseriesPersistenceManager {
    pub fn list_timeseries(&self) -> Result<Vec<String>> {
        let conn = self.connection.lock().unwrap();
        let mut stmt = conn.prepare("SELECT name FROM sqlite_master WHERE type='table';")?;
        let timeseries_iter = stmt.query_map([], |row| row.get(0))?;
        let mut timeseries = Vec::new();
        for timeseries_name in timeseries_iter {
            timeseries.push(timeseries_name?);
        }
        Ok(timeseries)
    }

    // pub async fn describe(&mut self, database: String) {
    //     let tables = self.storages.lock().unwrap();
    //     for k in tables.keys() {
    //         let qq = format!("SHOW COLUMNS FROM {}", k.clone());
    //         let tablename = k;
    //         match self.query(qq.clone(), tablename.to_string()).await {
    //             Ok(r) => {println!("{}", format!("{:?}", r));}
    //             Err(e) => {println!("{}", format!("{:?}", e));}
    //         }
    //     }
    // }

    pub fn timeseries_exists(&self, ts_name: &str) -> bool {
        let conn = self.connection.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT name FROM sqlite_master WHERE type='table' AND name=?1;")
            .unwrap();
        let exists = stmt.exists(params![ts_name]).unwrap();
        exists
    }

    pub fn create_timeseries(&self, timeseries_name: &str) -> Result<()> {
        let conn = self.connection.lock().unwrap();
        conn.execute(
            &format!(
                "CREATE TABLE IF NOT EXISTS {} (
                    id TEXT PRIMARY KEY,
                    time INTEGER,
                    created_at INTEGER,
                    name TEXT,
                    value REAL,
                    tags TEXT
                )",
                timeseries_name
            ),
            [],
        )?;
        Ok(())
    }

    // TODO: implement tags
    pub fn save_measurement(
        &self,
        timeseries_name: &str,
        name: &str,
        value: f64,
        tags: &HashMap<String, String>,
    ) -> Result<Measurement> {
        let uuid = Uuid::new_v4();
        let now = Local::now().timestamp_millis();
        let tags_json = serde_json::to_string(tags).unwrap();

        let conn = self.connection.lock().unwrap();
        conn.execute(
            &format!(
                "INSERT INTO {} (id, time, created_at, name, value, tags) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                timeseries_name
            ),
            params![uuid.to_string(), now, now, name, value, tags_json],
        )?;

        Ok(Measurement {
            id: uuid,
            time: now,
            created_at: now,
            name: name.to_string(),
            value,
            tags: tags.clone(),
        })
    }

    // consider this insecure by design. the timeseries name comes with the query string :grin:
    pub fn get_measurement_range(
        &self,
        timeseries_name: &str,
        start_key: i64,
        end_key: i64,
    ) -> Result<Vec<Measurement>> {
        self.query_measurements(timeseries_name, start_key, end_key)
    }

    pub fn query_measurements(
        &self,
        timeseries_name: &str,
        start_key: i64,
        end_key: i64,
    ) -> Result<Vec<Measurement>> {
        let conn = self.connection.lock().unwrap();
        let mut stmt = conn.prepare(
            &format!(
                "SELECT id, time, created_at, name, value, tags FROM {} WHERE time >= ?1 AND time <= ?2",
                timeseries_name
            )
        )?;
        let measurement_iter = stmt.query_map(params![start_key, end_key], |row| {
            Ok(Measurement {
                id: Uuid::parse_str(row.get::<_, String>(0)?.as_str()).unwrap(),
                time: row.get(1)?,
                created_at: row.get(2)?,
                name: row.get(3)?,
                value: row.get(4)?,
                tags: serde_json::from_str(row.get::<_, String>(5)?.as_str()).unwrap(),
            })
        })?;

        let mut measurements = Vec::new();
        for measurement in measurement_iter {
            measurements.push(measurement?);
        }
        Ok(measurements)
    }

    pub fn new(db_path: &str) -> Self {
        let connection = Connection::open(db_path).unwrap();
        TimeseriesPersistenceManager {
            connection: Arc::new(Mutex::new(connection)),
        }
    }
}

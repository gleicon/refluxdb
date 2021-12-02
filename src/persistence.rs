use chrono::Local;
use gluesql::executor::{EvaluateError, ExecuteError, FetchError};
use gluesql::prelude::*;

use crate::utils::db;
use log::{debug, info};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

// timeseries persistence manager
// Measurement: Unit of data tied to a time, within a timeseries, annotated with tags
// Timeseries: A set of immutable measurements that move forward in time
// Resultset: a slice of data from a timeseries within T(start) and T(end)
// Tags: json annotations to measurements used to filter and group resultsets
// Database structure:

// A sled database per timeseries, all within a root directory, abstracted by GlueSQL
//      pros: isolation, parallelism
//      cons: disk space, migration
// Immutable data: measurements can't be changed
// interface:
//      id -> UUID
//      time -> unix timestamp, ordered - measurement time (expanded 11-30-2021)
//      created_at -> unix timestamp, ordered, system time (added 11-30-2021)
//      name -> value name (added 11-30-2021)
//      value -> float (to be float, int, string and bool)
//      tags -> key/value tag map

// One Glue + Sled db per timeseries
// Table structure
// "CREATE TABLE <timeseries_name> (id UUID, time TIMESTAMP, created_at TIMESTAMP, name TEXT, value FLOAT, tags MAP);",
// TODO: separated tag table: "CREATE TABLE <timeseries_name>_tags (id UUID, key TEXT, value TEXT);",

// on start: read all databases in a folder, keep the handlers
// TODO: ensure immutability is enforced through measurement id or fingerprint
// TODO: Pre-calculated stats for each series

#[derive(Clone)]
pub struct TimeseriesDiskPersistenceManager {
    pub timeseries_path: HashMap<String, String>,
    pub storages: Arc<Mutex<HashMap<String, gluesql::storages::SledStorage>>>,
    pub basepath: String,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Measurement {
    pub key: i64, // A timestamp
    pub id: Uuid, // Unique ID for each measurement
    pub value: f64,
    // ts name -> ts db path
    pub tags: HashMap<String, String>,
}

impl TimeseriesDiskPersistenceManager {
    pub fn list_timeseries(self) -> Result<Vec<String>, String> {
        let databases: Vec<String> = self
            .storages
            .lock()
            .unwrap()
            .keys()
            .map(|dbname| dbname.clone())
            .collect();
        return Ok(databases.clone());
    }

    pub fn timeseries_exists(self, ts_name: String) -> bool {
        return self.timeseries_path.contains_key(&ts_name);
    }
    pub fn check_database(
        self,
        timeseries_name: String,
        create_if_not_exists: bool,
    ) -> Result<gluesql::storages::SledStorage, String> {
        let ss = self.storages.lock().unwrap();
        match ss.get(&timeseries_name.clone()) {
            Some(s) => Ok(s.clone()),
            None => {
                if create_if_not_exists {
                    let ts_path = format!("{}/{}", self.basepath, timeseries_name);
                    info!("Creating db {}", ts_path);

                    fs::create_dir(ts_path.clone());

                    return match self.load_or_create_database(ts_path) {
                        Ok(d) => info!("db {} created and checked", d),
                        Err(e) => info!("error creating db {}", e),
                    };
                };
                Err(format!("No storage found"))
            }
        }
    }

    // TODO: implement tags
    pub fn save_measurement(
        &mut self,
        timeseries_name: String,
        name: String,
        value: f64,
        tags: HashMap<String, String>,
        create_database: bool,
    ) -> Result<Measurement, String> {
        match self.clone().check_database(timeseries_name.clone(), true) {
            Ok(storage) => {
                let mut db = Glue::new(storage.clone());
                let uuid = Uuid::new_v4();
                let now = Local::now();
                let now_dt = now.to_rfc3339(); //timestamp_millis();
                let tags_json = serde_json::to_string(&tags);
                let query = format!(
                    // "CREATE TABLE {} (id UUID, time TIMESTAMP, created_at TIMESTAMP, name TEXT, value FLOAT, tags MAP);",
                    "INSERT INTO {} VALUES ('{}', '{}', '{}', '{}', {}, '{}')",
                    timeseries_name,
                    uuid, //.as_u128(),
                    now_dt,
                    now_dt,
                    name,
                    value,
                    tags_json.unwrap()
                );
                match db.execute(&query) {
                    Ok(result) => {
                        debug!("{:?}", result);
                        let ev = Measurement {
                            key: now.clone().timestamp_millis(),
                            id: uuid.clone(),
                            value: value.clone(),
                            tags: tags.clone(),
                        };
                        return Ok(ev);
                    }
                    Err(e) => {
                        return Err(format!("Error saving measurement: {} {}", e, query.clone()))
                    }
                }
            }
            Err(e) => return Err(format!("Error checking database {}", e)),
        };
    }

    // consider this insecure by design. the timeseries name comes with the query string :grin:
    pub fn query_measurements(&mut self, query: String) -> Result<Vec<Measurement>, String> {
        if query.to_uppercase().contains("INSERT")
            || query.to_uppercase().contains("DELETE")
            || query.to_uppercase().contains("UPDATE")
            || query.to_uppercase().contains("DROP")
            || query.to_uppercase().contains("CREATE")
        {
            return Err(format!("Invalid query {}", query));
        }
        match db::query_statement_tablename(query.clone()) {
            Ok(tablename) => {
                return self._run_query(tablename.clone(), query.clone());
            }
            Err(e) => {
                return Err(format!("Validator error: {}", e));
            }
        }
    }
    pub fn get_measurement_range(
        &mut self,
        timeseries_name: String,
        start_key: i64,
        end_key: i64,
    ) -> Result<Vec<Measurement>, String> {
        match self
            .clone()
            .check_database(timeseries_name.clone(), false) // only if the database exists, dont create it otherwise
            .clone()
        {
            Ok(storage) => {
                let mut db = Glue::new(storage.clone());
                let query = format!(
                    "SELECT key, id, created_at, name, value, tags from {} WHERE key >= {} AND key <= {}",
                    timeseries_name, start_key, end_key
                );
                // fetch or create the db handler
                match db.execute(&query) {
                    Ok(payload) => return db::parse_select_payload(payload),
                    Err(e) => match e {
                        gluesql::result::Error::Fetch(FetchError::TableNotFound(a)) => {
                            return Err(format!("Table not found: {}", a));
                        }
                        _ => {
                            return Err(format!("Error querying measurement: {}", timeseries_name));
                        }
                    },
                }
            }
            Err(e) => return Err(format!("Error checking database {}", e)),
        };
    }

    fn _run_query(&mut self, ts_name: String, query: String) -> Result<Vec<Measurement>, String> {
        let storage = self.storages.lock().unwrap().get(&ts_name).unwrap().clone();
        let mut db = Glue::new(storage.clone());
        match db.execute(&query) {
            Err(e) => match e {
                gluesql::result::Error::Fetch(FetchError::TableNotFound(a)) => {
                    return Err(format!("table fetch not found {:?}", a));
                }

                gluesql::result::Error::Execute(ExecuteError::TableNotFound(a)) => {
                    return Err(format!("table execute not found {:?}", a));
                }

                gluesql::result::Error::Evaluate(EvaluateError::ValueNotFound(a)) => {
                    return Err(format!("table evaluate not found {:?}", a));
                }
                _ => {
                    return Err(format!("query error: {:?}", e));
                }
            },
            Ok(payload) => db::parse_select_payload(payload),
        }
    }

    pub fn load_or_create_database(&mut self, timeseries_name: String) -> Result<bool, String> {
        let ts_tablename = timeseries_name.split("/").last().unwrap();

        match SledStorage::new(&timeseries_name.clone()) {
            Ok(ss) => {
                match db::check_or_create_database(ts_tablename.into(), ss.clone(), true) {
                    Ok(db) => {
                        self.storages
                            .lock()
                            .unwrap()
                            .insert(ts_tablename.into(), ss.clone());
                        self.timeseries_path
                            .insert(ts_tablename.into(), timeseries_name.clone());
                        info!(
                            "db name: {} path: {} - {}",
                            ts_tablename,
                            timeseries_name.clone(),
                            db
                        );
                    }
                    Err(e) => info!("Error checking db: {}", e),
                }
                Ok(true)
            }
            Err(e) => (return Err(format!("Error creating storage {}", e))),
        }
    }

    fn load_persistence(&mut self) {
        let dir = Path::new(&self.basepath);
        if dir.is_dir() {
            for entry in fs::read_dir(dir).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    let timeseries_name = path.to_str().unwrap().to_string();
                    info!(
                        "Loading databases basepath:{} - ts db:{:?}",
                        self.basepath,
                        timeseries_name.clone(),
                    );
                    self.load_or_create_database(timeseries_name).unwrap();
                };
            }
        }
    }

    pub fn setup(&mut self) {
        if !Path::new(&self.basepath).exists() {
            fs::create_dir_all(&self.basepath).unwrap();
            return;
        }

        self.load_persistence();
    }

    pub fn new(basepath: String) -> Self {
        let mut s = Self {
            basepath: basepath.clone(),
            timeseries_path: HashMap::new(),
            storages: Arc::new(Mutex::new(HashMap::new())),
        };
        s.setup();
        return s;
    }
}

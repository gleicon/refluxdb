use chrono::Local;
use gluesql::executor::{EvaluateError, ExecuteError, FetchError};
use gluesql::prelude::*;

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use uuid::Uuid;

// timeseries persistence manager
// Measurement: Unit of data tied to a time, within a timeseries, annotated with tags
// Timeseries: A set of immutable measurements that move forward in time
// Resultset: a slice of data from a timeseries within T(start) and T(end)
// Tags: json annotations to measurements used to filter and group resultsets
// Database structure:
//
// A sled database per timeseries, all within a root directory, abstracted by GlueSQL
//      pros: isolation, parallelism
//      cons: disk space, migration
// Immutable data: measurements can't be changed
// interface:
//      id -> UUID
//      time -> unix timestamp, ordered
//      value -> float (to be float, int, string and bool)
//      tags -> key/value tag map

// One Glue + Sled db per timeseries
// Table structure
// "CREATE TABLE <timeseries_name>_data (id UUID, time TIMESTAMP, value FLOAT, tags MAP);",
// TODO: separated tag table: "CREATE TABLE <timeseries_name>_tags (id UUID, key TEXT, value TEXT);",

// on start: read all databases in a folder, keep the handlers
// TODO: ensure immutability is enforced through measurement id or fingerprint

#[derive(Clone)]
pub struct TimeseriesDiskPersistenceManager {
    root_path: String,
    path: PathBuf,
    pub storages: Arc<Mutex<HashMap<String, gluesql::storages::SledStorage>>>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Measurement {
    pub key: i64, // A timestamp
    pub id: Uuid, // Unique ID for each measurement
    pub value: f64,
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

    // TODO: implement tags
    pub fn save_measurement(
        &mut self,
        timeseries_name: String,
        value: f64,
        tags: HashMap<String, String>,
    ) -> Result<Measurement, String> {
        let storage = self
            .storages
            .lock()
            .unwrap()
            .get(&timeseries_name.clone())
            .unwrap()
            .clone();

        let mut db = Glue::new(storage.clone());
        let uuid = Uuid::new_v4();
        let now = Local::now().timestamp_millis();
        let tags_json = serde_json::to_string(&tags);
        let query = format!(
            "INSERT INTO {} VALUES ({}, {}, {}, {})",
            timeseries_name,
            uuid,
            now,
            value,
            tags_json.unwrap()
        );
        match db.execute(&query) {
            Ok(result) => {
                let ev = Measurement {
                    key: now.clone(),
                    id: uuid.clone(),
                    value: value.clone(),
                    tags: tags.clone(),
                };
                return Ok(ev);
            }
            Err(e) => return Err(format!("Error saving measurement: {}", e)),
        }
    }

    pub fn _parse_select_payload(&mut self, payload: Payload) -> Result<Measurement, String> {
        let rows = match payload {
            Payload::Select { labels: _, rows } => rows,
            _ => return Err(format!("Unexpected result: {:?}", payload)),
        };
        if rows.len() == 0 {
            return Err(format!("No data found for query"));
        };
        self._parse_select_resultset_row(&rows[0])
    }

    pub fn _parse_select_payload_range(
        &mut self,
        payload: &Payload,
    ) -> Result<Vec<Measurement>, String> {
        let rows = match payload {
            Payload::Select { labels: _, rows } => rows,
            _ => return Err(format!("Unexpected result: {:?}", payload)),
        };
        let ev: Vec<Measurement> = Vec::new();

        if rows.len() == 0 {
            //return Err(format!("No data found for query"));
            return Ok(ev);
        };
        let mut ev: Vec<Measurement> = Vec::new();

        for row in rows {
            match self._parse_select_resultset_row(row) {
                Ok(e) => ev.push(e),
                Err(e) => return Err(format!("Error parsing range: {}", e)),
            }
        }
        Ok(ev)
    }

    pub fn _parse_select_resultset_row(
        &mut self,
        row: &std::vec::Vec<gluesql::data::Value>,
    ) -> Result<Measurement, String> {
        let key = match &row[0] {
            Value::I64(key) => key,
            val => return Err(format!("Unexpected value: {:?}", val)),
        };
        let id = match &row[1] {
            Value::Uuid(id) => id,
            val => return Err(format!("Unexpected value: {:?}", val)),
        };
        let value = match &row[2] {
            Value::F64(value) => value,
            val => return Err(format!("Unexpected value: {:?}", val)),
        };
        let tt = match &row[3] {
            Value::Map(tags) => tags,
            //   _ => HashMap::new(), // TODO: temp mock
            val => return Err(format!("Unexpected value: {:?}", val)),
        };
        Ok(Measurement {
            key: key.clone(),
            id: Uuid::from_u128(id.clone()),
            value: value.clone(),
            tags: HashMap::new(), //tt,
        })
    }
    pub fn get_measurement(
        &mut self,
        timeseries_name: String,
        time: i64,
    ) -> Result<Measurement, String> {
        let storage = self
            .storages
            .lock()
            .unwrap()
            .get(&timeseries_name.clone())
            .unwrap()
            .clone();
        let mut db = Glue::new(storage.clone());
        let query = format!(
            "SELECT key, id, value, tags from {} LIMIT 1",
            timeseries_name,
        );
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
            Ok(payload) => {
                return match self._parse_select_payload(payload) {
                    Ok(ev) => return Ok(ev),
                    Err(e) => Err(format!("Error parsing data: {}", e)),
                }
            }
        };
    }
    pub fn get_measurement_range(
        &mut self,
        timeseries_name: String,
        start_key: i64,
        end_key: i64,
    ) -> Result<Vec<Measurement>, String> {
        let storage = self
            .storages
            .lock()
            .unwrap()
            .get(&timeseries_name.clone())
            .unwrap()
            .clone();
        let mut db = Glue::new(storage.clone());
        let query = format!(
            "SELECT key, id, value, tags from {} WHERE key >= {} AND key <= {}",
            timeseries_name, start_key, end_key
        );
        // fetch or create the db handler
        match db.execute(&query) {
            Ok(payload) => match self._parse_select_payload(payload) {
                Ok(ev) => return Ok(vec![ev]),
                Err(e) => Err(format!("Error parsing data: {}", e)),
            },
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

    pub fn load_or_create_database(&mut self, timeseries_name: String) -> Result<bool, String> {
        let mut pb = PathBuf::new();
        pb.push(&self.path);
        pb.push(timeseries_name.clone());
        let path = pb.to_str();

        // wrap sledstorage around a new Glue obj before using it:
        // let mut database = Glue::new(storage);
        match SledStorage::new(&path.unwrap()) {
            Ok(ss) => {
                self.storages
                    .lock()
                    .unwrap()
                    .insert(timeseries_name.clone(), ss.clone());
                self._check_db_schema(timeseries_name.clone(), ss.clone(), true)
                    .unwrap();
                Ok(true)
            }
            // run CREATE statement if the table does not exists
            // "CREATE TABLE <timeseries_name>_data (id UUID, time TIMESTAMP, value FLOAT, tags MAP);"
            Err(e) => (return Err(format!("Error creating storage {}", e))),
        }
    }

    fn _check_db_schema(
        &mut self,
        timeseries_name: String,
        storage: gluesql::storages::SledStorage,
        create: bool,
    ) -> Result<String, String> {
        let mut db = Glue::new(storage.clone());
        let query = format!("SELECT * from {} LIMIT 1", timeseries_name,);
        match db.execute(&query) {
            Err(e) => match e {
                gluesql::result::Error::Fetch(FetchError::TableNotFound(a)) => {
                    if !create {
                        return Err(format!("table fetch not found {:?}", a));
                    } else {
                        // "CREATE TABLE <timeseries_name>_data (id UUID, time TIMESTAMP, value FLOAT, tags MAP);"
                        match db.execute("CREATE TABLE <timeseries_name>_data (id UUID, time TIMESTAMP, value FLOAT, tags MAP);") {
                            Err(ei) => { return Err(format!("Error creating table: {} - {}", timeseries_name, ei)); }
                            Ok(_) => {return Ok(format!("Database {} created", timeseries_name.clone()));}
                        };
                    }
                }
                _ => {
                    return Err(format!("query error: {:?}", e));
                }
            },
            _ => return Ok(format!("Database {} loaded", timeseries_name.clone())),
        };
    }

    fn load_persistence(&mut self) {
        let dir = &self.path;
        if dir.is_dir() {
            for entry in fs::read_dir(dir).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    let timeseries_name = path.to_str().unwrap().to_string();
                    self.load_or_create_database(timeseries_name).unwrap();
                };
            }
        }
    }

    pub fn setup(&mut self) {
        if !Path::new(&self.root_path).exists() {
            fs::create_dir_all(&self.root_path).unwrap();
        }

        self.load_persistence();
    }

    pub fn new(basepath: String) -> Self {
        let bp = Path::new(&basepath);
        let mut s = Self {
            root_path: basepath.clone(),
            storages: Arc::new(Mutex::new(HashMap::new())),
            path: bp.to_path_buf(),
        };
        s.setup();
        return s;
    }
}

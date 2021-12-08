use arrow::record_batch::RecordBatch;
use chrono::Local;
use futures::StreamExt;

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
//
// Database structure:
// a basepath containing parquet folders and files partitioned by time
// this is an alternate take on gluesql + sled in favor of arrow and datafusion
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
    pub basepath: String,
    pub storages: Arc<Mutex<HashMap<String, crate::utils::filemanager::ParquetFileManager>>>,
    pub execution_contexts: Arc<Mutex<HashMap<String, datafusion::prelude::ExecutionContext>>>,
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

    pub async fn describe(&mut self, database: String) {
        for (k, _v) in self.storages.lock().unwrap().iter() {
            let qq = format!("SHOW COLUMNS FROM {}", k.clone());
            println!(
                "{}",
                format!(
                    "db: {} - columns {}",
                    k.clone(),
                    self.query(qq).await.unwrap()
                )
            );
        }
    }

    pub fn timeseries_exists(self, ts_name: String) -> bool {
        return self.storages.lock().unwrap().contains_key(&ts_name);
    }

    pub fn check_database(
        self,
        timeseries_name: String,
        create_if_not_exists: bool,
    ) -> Result<crate::utils::filemanager::ParquetFileManager, String> {
        let ss = self.storages.lock().unwrap();
        match ss.get(&timeseries_name.clone()) {
            Some(s) => Ok(s.clone()),
            None => {
                if create_if_not_exists {
                    let ts_path = format!("{}/{}", self.basepath, timeseries_name);
                    info!("Creating db {}", ts_path);

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
    pub async fn save_measurement(
        &mut self,
        timeseries_name: String,
        name: String,
        value: f64,
        tags: HashMap<String, String>,
        create_database: bool,
    ) -> Result<Measurement, String> {
        match self.clone().check_database(timeseries_name.clone(), true) {
            Ok(dbe) => {
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

                match self
                    .write_to_parquet(timeseries_name, &query, dbe.path.to_str().unwrap())
                    .await
                {
                    Ok(r) => {
                        debug!("{:?}", r);
                        let ev = Measurement {
                            time: now.clone().timestamp_millis(),
                            created_at: now.clone().timestamp_millis(),
                            name: name,
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

    pub async fn write_to_parquet(
        &mut self,
        timeseries: String,
        query: &str,
        filepath: &str,
    ) -> datafusion::error::Result<()> {
        let ctx = self
            .execution_contexts
            .lock()
            .unwrap()
            .get(&timeseries)
            .unwrap()
            .clone();
        let logical_plan = ctx.create_logical_plan(&query).unwrap();
        let logical_plan = ctx.optimize(&logical_plan).unwrap();
        let physical_plan = ctx.create_physical_plan(&logical_plan).await.unwrap();
        ctx.write_parquet(physical_plan, filepath, None).await
    }

    // consider this insecure by design. the timeseries name comes with the query string :grin:
    pub async fn query_measurements(&mut self, query: String) -> Result<Vec<RecordBatch>, String> {
        if query.to_uppercase().contains("INSERT")
            || query.to_uppercase().contains("DELETE")
            || query.to_uppercase().contains("UPDATE")
            || query.to_uppercase().contains("DROP")
            || query.to_uppercase().contains("CREATE")
        {
            return Err(format!("Invalid query {}", query.clone()));
        }

        match db::query_statement_tablename(query.clone()) {
            Ok(tablename) => {
                let r = self.query(tablename, query.clone()).await;
                return Ok(r.unwrap());
            }
            Err(e) => {
                return Err(format!("Validator error: {}", e));
            }
        }
    }
    pub async fn get_measurement_range(
        &mut self,
        timeseries_name: String,
        start_key: i64,
        end_key: i64,
    ) -> Result<Vec<RecordBatch>, String> {
        match self
            .clone()
            .check_database(timeseries_name.clone(), false) // only if the database exists, dont create it otherwise
            .clone()
        {
            Ok(db) => {
                let query = format!(
                    "SELECT key, id, created_at, name, value, tags from {} WHERE key >= {} AND key <= {}",
                    timeseries_name.clone(), start_key, end_key
                );
                // fetch or create the db handler
                match self.query(timeseries_name.clone(), query).await {
                    Ok(payload) => return Ok(payload), //return db::parse_select_payload(payload),
                    Err(e) => {
                        return Err(format!(
                            "Error querying measurement: {}",
                            timeseries_name.clone()
                        ))
                    }
                };
            }
            Err(e) => return Err(format!("Error checking database {}", e)),
        };
    }

    pub async fn query(
        &mut self,
        tablename: String,
        query: String,
    ) -> Result<Vec<RecordBatch>, String> {
        let ctx = self
            .execution_contexts
            .lock()
            .unwrap()
            .get(&tablename)
            .unwrap();
        println!("query: {}", query);
        // cargo run -- -d -f test_data/taxi_2019_04.parquet -q "SELECT count(*) FROM parquet_tables"
        match ctx.sql(&query).await {
            Ok(df) => {
                let results: Vec<RecordBatch> = df.collect().await.unwrap();

                return Ok(results);
            }
            Err(e) => return Err(format!("Error querying: {}", e)),
        }
    }

    pub fn load_or_create_database(&mut self, timeseries_name: String) -> Result<bool, String> {
        let ts_tablename = timeseries_name.split("/").last().unwrap();

        let execution_config =
            datafusion::prelude::ExecutionConfig::new().with_information_schema(true);

        let mut pfm = crate::utils::ParquetFileManager::new(self.basepath.clone());
        self.storages
            .lock()
            .unwrap()
            .insert(ts_tablename.into(), pfm.clone());
        self.execution_contexts
            .lock()
            .unwrap()
            .insert(ts_tablename.into(), pfm.execution_context.clone());
        Ok(true)
    }

    fn load_persistence(&mut self) {
        let dir = Path::new(&self.basepath);
        if dir.is_dir() {
            for entry in fs::read_dir(dir).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    let timeseries_name = path.to_str().unwrap().to_string();
                    info!(
                        "Loading databases basepath: {} - parquet ts db:{:?}",
                        self.basepath,
                        timeseries_name.clone(),
                    );
                    self.load_or_create_database(timeseries_name).unwrap();
                };
            }
        }
    }

    pub fn setup(&mut self) {
        // create folder if it does not exists
        if !Path::new(&self.basepath).exists() {
            fs::create_dir_all(&self.basepath).unwrap();
            return;
        }
        self.load_persistence();
    }

    pub fn new(basepath: String) -> Self {
        let bp = Path::new(&basepath);

        let mut s = Self {
            basepath: basepath.clone(),
            storages: Arc::new(Mutex::new(HashMap::new())),
            execution_contexts: Arc::new(Mutex::new(HashMap::new())),
        };
        s.setup();
        return s;
    }
}

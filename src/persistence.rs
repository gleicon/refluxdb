use arrow::record_batch::RecordBatch;
use chrono::Local;

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

    pub fn timeseries_exists(self, ts_name: String) -> bool {
        return self.storages.lock().unwrap().contains_key(&ts_name);
    }

    pub async fn check_database(
        self,
        timeseries_name: String,
        create_if_not_exists: bool,
    ) -> Result<crate::utils::filemanager::ParquetFileManager, String> {
        match self
            .storages
            .lock()
            .unwrap()
            .get_mut(&timeseries_name.clone())
        {
            // deadlock, item is in, db does not exists
            Some(s) => Ok(s.clone()),
            None => {
                if create_if_not_exists {
                    let ts_path = format!("{}/{}", self.basepath, timeseries_name);
                    info!("Creating db {}", ts_path);
                    match self.clone().load_or_create_database(ts_path.clone()).await {
                        Ok(d) => return Ok(d),
                        Err(e) => return Err(format!("error creating db {}", e)),
                    };
                };
                return Err(format!("No storage found"));
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
        match self
            .clone()
            .check_database(timeseries_name.clone(), create_database)
            .await
        {
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
        let st = self
            .storages
            .lock()
            .unwrap()
            .get(&timeseries)
            .unwrap()
            .clone();
        let ctx = st.execution_context.clone();
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
                match self.storages.lock().unwrap().get_mut(&tablename) {
                    Some(pfm) => {
                        return Ok(pfm
                            .execution_context
                            .sql(&query)
                            .await
                            .unwrap()
                            .collect()
                            .await
                            .unwrap());
                        // match pfm.execution_context.sql(&query).await{
                        //     Ok(df) => {
                        //         return Ok(df.collect().await.unwrap());
                        //     },
                        //     Err(e) => return Err(format!("Error querying: {}", e)),
                        // }
                    }
                    None => return Err(format!("Error querying")),
                }
                // let r = self.query(tablename, query.clone()).await;
                // return Ok(r.unwrap());
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
            .check_database(timeseries_name.clone(), false)
            .await
            .clone()
        {
            Ok(db) => {
                let query = format!(
                    "SELECT key, id, created_at, name, value, tags from {} WHERE key >= {} AND key <= {}",
                    timeseries_name.clone(), start_key, end_key
                );
                // fetch or create the db handler
                match db.clone().execution_context.sql(&query).await {
                    Ok(df) => {
                        return Ok(df.collect().await.unwrap());
                    }
                    Err(e) => return Err(format!("Error querying: {}", e)),
                }
            }
            Err(e) => return Err(format!("Error checking database {}", e)),
        };
    }

    pub async fn load_or_create_database(
        self,
        timeseries_name: String,
    ) -> Result<crate::utils::filemanager::ParquetFileManager, String> {
        let ts_tablename = timeseries_name.split("/").last().unwrap();

        match crate::utils::ParquetFileManager::new(self.basepath.clone(), true).await {
            Ok(pfm) => {
                self.storages
                    .lock()
                    .unwrap()
                    .insert(ts_tablename.into(), pfm.clone());
                return Ok(pfm.clone());
            }
            Err(e) => Err(format!("Error: {}", e)),
        }
    }

    async fn load_persistence(self) {
        let dir = Path::new(&self.basepath);
        if dir.is_dir() {
            for entry in fs::read_dir(dir).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    let timeseries_name = path.to_str().unwrap().to_string();
                    info!(
                        "Loading databases basepath: {:?} - parquet ts db:{:?}",
                        dir,
                        timeseries_name.clone(),
                    );
                    match self.clone().load_or_create_database(timeseries_name).await {
                        Ok(db) => println!("Db loaded"),
                        Err(e) => println!("Err: no table at {:?} - {}", dir, e),
                    }
                };
            }
        }
    }

    pub async fn setup(self) {
        // create folder if it does not exists
        if !Path::new(&self.basepath).exists() {
            fs::create_dir_all(&self.basepath).unwrap();
            return;
        }
        self.clone().load_persistence().await;
    }

    pub async fn new(basepath: String) -> Self {
        let s = Self {
            basepath: basepath.clone(),
            storages: Arc::new(Mutex::new(HashMap::new())),
        };
        s.clone().setup().await;
        return s;
    }
}

use chrono::Local;
use serde::{Deserialize, Serialize};
use sled::Config;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

// timeseries persistence manager
// structure:
//  one sled database per timeseries, all within a root directory
//      pros: isolation, parallelism
//      cons: disk space, migration
// immutable structure
// interface:
//  key -> unix timestamp, ordered
//  value -> envelope with:
//      auto_id -> db generated automatic id
//      Vec<u8> value
//      Hashmap<String, String> annotations (triggers and tags)
//  Vec<u8> in and out
//  value bincode serde

// on start: read all databases in a folder, keep the handlers
// TODO: on key change: watch_prefix per table w/ watcher registration
// TODO: stats: disk space, queue number, queue sizes
// TODO: LRU tables (expire/delete old messages)

// #[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
// struct Envelope {
//     body: String, //Vec<u8>,
// }

#[derive(Clone)]
pub struct TimeseriesDiskPersistenceManager {
    root_path: String,
    path: PathBuf,
    databases: HashMap<String, sled::Db>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct TimestampEnvelope {
    pub key: i64,
    pub id: Vec<u8>,
    pub value: Vec<u8>,
    pub created_at: i64,
    pub tags: HashMap<String, String>,
}

impl TimeseriesDiskPersistenceManager {
    // TODO: implement tags
    pub fn save_element(
        &mut self,
        timeseries_name: String,
        body: Vec<u8>,
    ) -> Result<TimestampEnvelope, String> {
        let mut db = self.databases.get(&timeseries_name.clone());
        match db.clone() {
            Some(_) => (),
            None => {
                self.load_or_create_database(timeseries_name.clone())
                    .unwrap();
                db = self.databases.get(&timeseries_name);
            }
        }

        let db = db.unwrap();
        let autoid = format!("{}:{}", timeseries_name, db.generate_id().unwrap());
        let dt = Local::now();
        let key = format!("{}", dt.timestamp_millis());

        let ev = TimestampEnvelope {
            key: dt.timestamp_millis(),
            id: autoid.as_bytes().to_vec(),
            value: body.clone(),
            created_at: dt.timestamp_millis(),
            tags: HashMap::new(),
        };

        let encoded: Vec<u8> = bincode::serialize(&ev).unwrap();
        let res = db.insert(key.clone(), encoded.clone());

        match res {
            Ok(Some(b)) => Err(format!("Error - element already exists {:?}", b)),
            Ok(None) => Ok(ev),
            Err(e) => Err(format!("Error persisting message: {}", e)),
        }
    }

    // TODO: that should die, the table should be immutable (no pop or update)
    pub fn pop_newest_element(
        &mut self,
        timeseries_name: String,
    ) -> Result<TimestampEnvelope, String> {
        let db = self.databases.get(&timeseries_name.clone());

        // fetch or create the db handler
        let dbc = match db.clone() {
            Some(_) => (db),
            None => {
                self.load_or_create_database(timeseries_name.clone())
                    .unwrap();
                self.databases.get(&timeseries_name)
            }
        };

        let dbc = dbc.unwrap();

        match dbc.pop_max().unwrap() {
            Some((_, value)) => Ok(bincode::deserialize(value.as_ref()).unwrap()),
            None => Err("Empty table".to_string()),
        }
    }

    pub fn get_element(
        &mut self,
        timeseries_name: String,
        key: String,
    ) -> Result<TimestampEnvelope, String> {
        let db = self.databases.get(&timeseries_name.clone());
        // fetch or create the db handler
        let dbc = match db.clone() {
            Some(_) => (db),
            None => {
                self.load_or_create_database(timeseries_name.clone())
                    .unwrap();
                self.databases.get(&timeseries_name)
            }
        };

        let dbc = dbc.unwrap();

        match dbc.get(key).unwrap() {
            Some(value) => Ok(bincode::deserialize(value.as_ref()).unwrap()),
            None => Err("Empty queue table".to_string()),
        }
    }

    pub fn get_element_range(
        &mut self,
        timeseries_name: String,
        start_key: String,
        end_key: String,
    ) -> Result<Vec<TimestampEnvelope>, String> {
        let db = self.databases.get(&timeseries_name.clone());
        // fetch or create the db handler
        let dbc = match db.clone() {
            Some(_) => (db),
            None => {
                self.load_or_create_database(timeseries_name.clone())
                    .unwrap();
                self.databases.get(&timeseries_name)
            }
        };
        let mut tev: Vec<TimestampEnvelope> = Vec::new();
        let dbc_iter = dbc.unwrap().range(start_key..end_key);

        for val in dbc_iter {
            match val {
                Ok(v) => tev.push(bincode::deserialize(v.1.as_ref()).unwrap()),
                Err(e) => return Err(format!("Error querying table for {}", e)),
            }
        }
        Ok(tev)
    }

    pub fn load_or_create_database(&mut self, timeseries_name: String) -> Result<bool, String> {
        let mut pb = PathBuf::new();
        pb.push(&self.path);
        pb.push(timeseries_name.clone());
        let config = Config::new().path(pb);
        match config.open() {
            Ok(d) => {
                self.databases.insert(timeseries_name, d);
                Ok(true)
            }
            Err(e) => Err(format!("Error opening database: {:?}", e)),
        }
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
            databases: HashMap::new(),
            path: bp.to_path_buf(),
        };
        s.setup();
        return s;
    }
}

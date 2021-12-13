use datafusion;
use std::path::{Path, PathBuf};

#[derive(Clone)]
pub struct ParquetFileManager {
    pub root_path: String,
    pub path: PathBuf,
    pub execution_context: datafusion::prelude::ExecutionContext,
}

impl ParquetFileManager {
    async fn load_files(&mut self) -> Result<(), String> {
        let main_name = &self.path.file_stem().unwrap().to_str().unwrap();
        match self
            .execution_context
            .register_parquet(main_name, &self.root_path)
            .await
        {
            Ok(_r) => return Ok(()),
            Err(e) => return Err(format!("Error registering parquet: {}", e)),
        }
    }

    async fn create_empty_parquet(&mut self) {
        let timeseries_schema = "
        timeseries schema {
            REQUIRED INT32 b;
        }
        ";
    }
    pub async fn new(basepath: String, create_if_not_exists: bool) -> Result<Self, String> {
        let bp = Path::new(&basepath);
        let execution_config =
            datafusion::prelude::ExecutionConfig::new().with_information_schema(true);

        let mut s = Self {
            root_path: basepath.clone(),
            path: bp.to_path_buf(),
            execution_context: datafusion::prelude::ExecutionContext::with_config(execution_config),
        };

        match s.load_files().await {
            Ok(_) => return Ok(s),
            Err(e) => {
                if !create_if_not_exists {
                    // create an empty parquet file
                    return Err(format!("Error loading parquet files: {}", e));
                }
            }
        }
        return Ok(s);
    }
}

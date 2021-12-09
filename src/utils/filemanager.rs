use datafusion;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Clone)]
pub struct ParquetFileManager {
    pub root_path: String,
    pub path: PathBuf,
    pub execution_context: datafusion::prelude::ExecutionContext,
}

impl ParquetFileManager {
    async fn load_parquet(&mut self, pathname: String) {
        let path = Path::new(&pathname);
        let tablename = path.file_stem().unwrap().to_str().unwrap();

        self.execution_context
            .register_parquet(&tablename.clone(), &pathname)
            .await
            .unwrap();
    }

    async fn load_files(&mut self) {
        let dir = &self.path;
        if dir.is_dir() {
            // directory with one or more files may be a partitioned parquet file
            for entry in fs::read_dir(dir).unwrap() {
                let path = entry.unwrap().path();
                if path.is_file() {
                    let parquet_path = path.to_str().unwrap().to_string();
                    self.load_parquet(parquet_path).await;
                };
                let main_name = self.path.file_stem().unwrap().to_str().unwrap();
                self.execution_context
                    .register_parquet(main_name, &self.root_path)
                    .await
                    .unwrap();
            }
        } else {
            // single file
            if self.path.is_file() {
                let parquet_path = self.path.to_str().unwrap().to_string();
                self.load_parquet(parquet_path).await;
            };
        }
    }

    pub async fn new(basepath: String) -> Self {
        let bp = Path::new(&basepath);
        let execution_config =
            datafusion::prelude::ExecutionConfig::new().with_information_schema(true);

        let mut s = Self {
            root_path: basepath.clone(),
            path: bp.to_path_buf(),
            execution_context: datafusion::prelude::ExecutionContext::with_config(execution_config),
        };

        s.load_files().await;
        return s;
    }
}

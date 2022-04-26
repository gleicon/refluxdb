use datafusion;
use parquet::{
    file::{
        properties::WriterProperties,
        writer::{FileWriter, SerializedFileWriter},
    },
    schema::parser::parse_message_type,
};
use parquet::{column::writer::ColumnWriter, data_type::ByteArray};

use std::{fs, convert::TryFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::persistence::Measurement;

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
        // (id UUID, time TIMESTAMP, created_at TIMESTAMP, name TEXT, value FLOAT, tags MAP);",
        // https://parquet.apache.org/documentation/latest/
        // map timeseries to parquet type
        let timeseries_schema = "
        message schema {
            REQUIRED BYTE_ARRAY id;
            REQUIRED INT64 time;
            REQUIRED INT64 created_at;
            REQUIRED BYTE_ARRAY name;
            REQUIRED value FLOAT;
            REQUIRED tags BYTE_ARRAY;
        }
        ";
        let schema = Arc::new(parse_message_type(timeseries_schema).unwrap());
        let props = Arc::new(WriterProperties::builder().build());
        let file = fs::File::create(self.path.clone()).unwrap();
        let mut writer = SerializedFileWriter::new(file, schema, props).unwrap();
        let row_group_writer = writer.next_row_group().unwrap();
        // while let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
        //     // ... write values to a column writer
        //     row_group_writer.close_column(col_writer).unwrap();
        // }
        writer.close_row_group(row_group_writer).unwrap();
        writer.close().unwrap();
    }

    pub async fn write_parquet(&mut self, ev: &Measurement) {
        // (id UUID, time TIMESTAMP, created_at TIMESTAMP, name TEXT, value FLOAT, tags MAP);",
        // https://parquet.apache.org/documentation/latest/
        // map timeseries to parquet type
        let timeseries_schema = "
        message schema {
            REQUIRED BYTE_ARRAY id;
            REQUIRED INT64 time;
            REQUIRED INT64 created_at;
            REQUIRED BYTE_ARRAY name;
            REQUIRED FLOAT value;
            REQUIRED BYTE_ARRAY tags;
        }
        ";
        let mut filename = self.path.clone();
        //filename.push(ev.name.as_str());
        filename.push(ev.name.as_str());
        let schema = Arc::new(parse_message_type(timeseries_schema).unwrap());
        let props = Arc::new(WriterProperties::builder().build());
        let file = fs::File::create(filename).unwrap();
        let mut writer = SerializedFileWriter::new(file, schema, props).unwrap();
        let mut row_group_writer = writer.next_row_group().unwrap();
        // Columns:

        // BYTE_ARRAY
        // INT64
        // INT64
        // FLOAT
        // BYTE_ARRAY
        // ****** id (uuid)
        let id_writer = row_group_writer.next_column().unwrap();
            if let Some(mut writer) = id_writer {
                match writer {
                    ColumnWriter::ByteArrayColumnWriter(ref mut typed) => {
                        let ba = ByteArray::from(ev.id.to_string().as_str());
                        let values = vec![ba];
                        let _ = typed.write_batch(&values, None, None).unwrap() as i64;

                    },
                    _ => {
                        unimplemented!();
                    }
                }
                row_group_writer.close_column(writer).unwrap();
            }
        // ****** time
        let data_writer = row_group_writer.next_column().unwrap();
            if let Some(mut writer) = data_writer {
                match writer {
                    ColumnWriter::Int64ColumnWriter(ref mut typed) => {
                        let values = vec![ev.time];
                        let _ = typed.write_batch(&values, None, None).unwrap() as i64;
                    }
                    _ => {
                        unimplemented!();
                    }
                }
                row_group_writer.close_column(writer).unwrap();
            }
         // ******

         // ****** creates at
        let mut created_at_writer = row_group_writer.next_column().unwrap();
        if let Some(mut writer) = created_at_writer {
            match writer {
                ColumnWriter::Int64ColumnWriter(ref mut typed) => {
                    let values = vec![ev.created_at];
                    let _ = typed.write_batch(&values, None, None).unwrap() as i64;
                }
                _ => {
                    unimplemented!();
                }
            }
            row_group_writer.close_column(writer).unwrap();
        }
     // ******

       // ****** value (float)
       let value_writer = row_group_writer.next_column().unwrap();
       if let Some(mut writer) = value_writer {
           match writer {
               ColumnWriter::DoubleColumnWriter(ref mut typed) => {
                   let values = vec![ev.value];
                   let _ = typed.write_batch(&values, None, None).unwrap() as i64;
               }
                   _ => {
                       unimplemented!();
                   }
           }
           row_group_writer.close_column(writer).unwrap();
       }
    // ******

    // *** tags
    let mut tags_writer = row_group_writer.next_column().unwrap();
            if let Some(mut writer) = tags_writer {
                match writer {
                    ColumnWriter::ByteArrayColumnWriter(ref mut typed) => {
                        //let ba = ByteArray::try_from(ev.id.as_bytes());
                        let ba = ByteArray::try_from(bincode::serialize(&ev.tags).unwrap());
                        let values = vec![ba.unwrap()];
                        let _ = typed.write_batch(&values, None, None).unwrap() as i64;

                    },
                    _ => {
                        unimplemented!();
                    }
                }
                row_group_writer.close_column(writer).unwrap();
            }
    // ***

       
        writer.close_row_group(row_group_writer).unwrap();
        writer.close().unwrap();
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
                    s.create_empty_parquet().await;
                    return Err(format!("Error loading parquet files: {}", e));
                }
            }
        }
        return Ok(s);
    }
}

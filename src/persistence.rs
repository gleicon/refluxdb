use chrono::{DateTime, Utc};
use rusqlite::{Connection, Result};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::sql::QueryPlan;

#[derive(Debug, Serialize, Deserialize)]
pub struct Point {
    pub measurement: String,
    pub tags: std::collections::HashMap<String, String>,
    pub fields: std::collections::HashMap<String, f64>,
    #[serde(with = "chrono::serde::ts_seconds")]
    pub timestamp: DateTime<Utc>,
}

pub struct TimeseriesPersistenceManager {
    conn: Arc<Mutex<Connection>>,
}

impl TimeseriesPersistenceManager {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let conn = Connection::open(path)?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS points (
                id INTEGER PRIMARY KEY,
                measurement TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                tags TEXT NOT NULL,
                fields TEXT NOT NULL
            )",
            [],
        )?;
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_measurement ON points(measurement)",
            [],
        )?;
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_timestamp ON points(timestamp)",
            [],
        )?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    pub async fn write_point(&self, point: Point) -> Result<()> {
        let conn = self.conn.lock().await;
        let tags = serde_json::to_string(&point.tags)
            .map_err(|e| rusqlite::Error::InvalidParameterName(e.to_string()))?;
        let fields = serde_json::to_string(&point.fields)
            .map_err(|e| rusqlite::Error::InvalidParameterName(e.to_string()))?;
        conn.execute(
            "INSERT INTO points (measurement, timestamp, tags, fields) VALUES (?1, ?2, ?3, ?4)",
            [
                point.measurement,
                point.timestamp.timestamp().to_string(),
                tags,
                fields,
            ],
        )?;
        Ok(())
    }

    pub async fn query(&self, plan: &QueryPlan) -> Result<Vec<Point>> {
        let conn = self.conn.lock().await;
        let mut query = String::from(
            "SELECT measurement, timestamp, tags, fields FROM points WHERE measurement = ?1",
        );
        let mut params: Vec<&dyn rusqlite::ToSql> = vec![&plan.measurement];
        let mut time_values = Vec::new();

        if let Some((start, end)) = plan.time_range {
            query.push_str(" AND timestamp >= ?2 AND timestamp <= ?3");
            time_values.push(start.to_string());
            time_values.push(end.to_string());
            params.push(&time_values[0]);
            params.push(&time_values[1]);
        }

        for (key, value) in &plan.tags {
            query.push_str(&format!(
                " AND json_extract(tags, '$.{}') = ?{}",
                key,
                params.len() + 1
            ));
            params.push(value);
        }

        if let Some(limit) = plan.limit {
            query.push_str(&format!(" LIMIT {}", limit));
        }

        let mut stmt = conn.prepare(&query)?;
        let mut rows = stmt.query(params.as_slice())?;
        let mut points = Vec::new();

        while let Some(row) = rows.next()? {
            let measurement: String = row.get(0)?;
            let timestamp: i64 = row.get(1)?;
            let tags: String = row.get(2)?;
            let fields: String = row.get(3)?;

            let tags: std::collections::HashMap<String, String> = serde_json::from_str(&tags)
                .map_err(|e| rusqlite::Error::InvalidParameterName(e.to_string()))?;
            let fields: std::collections::HashMap<String, f64> = serde_json::from_str(&fields)
                .map_err(|e| rusqlite::Error::InvalidParameterName(e.to_string()))?;

            points.push(Point {
                measurement,
                tags,
                fields,
                timestamp: DateTime::from_timestamp(timestamp, 0)
                    .unwrap()
                    .with_timezone(&Utc),
            });
        }

        Ok(points)
    }

    pub async fn list_timeseries(&self) -> Result<Vec<String>> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare("SELECT DISTINCT measurement FROM points")?;
        let mut rows = stmt.query([])?;
        let mut measurements = Vec::new();

        while let Some(row) = rows.next()? {
            let measurement: String = row.get(0)?;
            measurements.push(measurement);
        }

        Ok(measurements)
    }

    pub async fn timeseries_exists(&self, name: &str) -> Result<bool> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare("SELECT 1 FROM points WHERE measurement = ?1 LIMIT 1")?;
        let exists = stmt.query([name])?.next()?.is_some();
        Ok(exists)
    }

    pub async fn save_measurement(
        &self,
        measurement: &str,
        field: &str,
        value: f64,
        tags: &std::collections::HashMap<String, String>,
    ) -> Result<()> {
        let conn = self.conn.lock().await;
        let tags = serde_json::to_string(tags)
            .map_err(|e| rusqlite::Error::InvalidParameterName(e.to_string()))?;
        let fields = serde_json::to_string(&std::collections::HashMap::from([(
            field.to_string(),
            value,
        )]))
        .map_err(|e| rusqlite::Error::InvalidParameterName(e.to_string()))?;

        conn.execute(
            "INSERT INTO points (measurement, timestamp, tags, fields) VALUES (?1, ?2, ?3, ?4)",
            [
                measurement,
                &chrono::Utc::now().timestamp().to_string(),
                &tags,
                &fields,
            ],
        )?;
        Ok(())
    }

    pub async fn get_measurement_range(
        &self,
        measurement: &str,
        start: i64,
        end: i64,
    ) -> Result<Vec<(i64, f64)>> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare(
            "SELECT timestamp, json_extract(fields, '$.value') FROM points WHERE measurement = ?1 AND timestamp >= ?2 AND timestamp <= ?3 ORDER BY timestamp",
        )?;
        let mut rows = stmt.query([measurement, &start.to_string(), &end.to_string()])?;
        let mut results = Vec::new();

        while let Some(row) = rows.next()? {
            let timestamp: i64 = row.get(0)?;
            let value: f64 = row.get(1)?;
            results.push((timestamp, value));
        }

        Ok(results)
    }

    pub async fn query_measurements(
        &self,
        query: &str,
        start: i64,
        end: i64,
    ) -> Result<Vec<(i64, f64)>> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare(
            "SELECT timestamp, json_extract(fields, '$.value') FROM points WHERE timestamp >= ?1 AND timestamp <= ?2 ORDER BY timestamp",
        )?;
        let mut rows = stmt.query([&start.to_string(), &end.to_string()])?;
        let mut results = Vec::new();

        while let Some(row) = rows.next()? {
            let timestamp: i64 = row.get(0)?;
            let value: f64 = row.get(1)?;
            results.push((timestamp, value));
        }

        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_write_and_query() {
        let dir = tempdir().unwrap();
        let db = TimeseriesPersistenceManager::new(dir.path().join("test.db")).unwrap();

        let mut tags = std::collections::HashMap::new();
        tags.insert("sensor".to_string(), "sensor1".to_string());

        let mut fields = std::collections::HashMap::new();
        fields.insert("value".to_string(), 42.0);

        let point = Point {
            measurement: "temperature".to_string(),
            tags,
            fields,
            timestamp: Utc::now(),
        };

        db.write_point(point).await.unwrap();

        let plan = QueryPlan {
            measurement: "temperature".to_string(),
            fields: vec!["value".to_string()],
            tags: std::collections::HashMap::new(),
            time_range: None,
            limit: None,
        };

        let results = db.query(&plan).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].fields.get("value").unwrap(), &42.0);
    }
}

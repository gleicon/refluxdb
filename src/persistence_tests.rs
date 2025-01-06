#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use uuid::Uuid;

    #[test]
    fn test_create_timeseries() {
        let db_path = ":memory:";
        let manager = TimeseriesPersistenceManager::new(db_path);
        let timeseries_name = "test_series";

        assert!(manager.create_timeseries(timeseries_name).is_ok());
        assert!(manager.timeseries_exists(timeseries_name));
    }

    #[test]
    fn test_save_and_query_measurement() {
        let db_path = ":memory:";
        let manager = TimeseriesPersistenceManager::new(db_path);
        let timeseries_name = "test_series";
        manager.create_timeseries(timeseries_name).unwrap();

        let mut tags = HashMap::new();
        tags.insert("location".to_string(), "office".to_string());

        let measurement = manager
            .save_measurement(timeseries_name, "temperature", 23.5, &tags)
            .unwrap();

        let results = manager
            .query_measurements(timeseries_name, measurement.time, measurement.time)
            .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, measurement.id);
        assert_eq!(results[0].name, "temperature");
        assert_eq!(results[0].value, 23.5);
        assert_eq!(results[0].tags, tags);
    }
}

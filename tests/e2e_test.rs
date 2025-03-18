use std::net::UdpSocket;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use actix_web::{test, web, App};
use serde_json::json;
use tempfile::NamedTempFile;

use refluxdb::handlers;
use refluxdb::persistence::TimeseriesPersistenceManager;
use refluxdb::udpserver::UDPRefluxServer;

async fn setup_test_server() -> (String, Arc<Mutex<TimeseriesPersistenceManager>>) {
    let temp_file = NamedTempFile::new().unwrap();
    let db_path = temp_file.path().to_str().unwrap();
    let pm = Arc::new(Mutex::new(
        TimeseriesPersistenceManager::new(db_path).unwrap(),
    ));

    let http_addr = "127.0.0.1:8080".to_string();
    let pm_clone = pm.clone();

    // Start HTTP server in background
    let server = actix_web::HttpServer::new(move || {
        actix_web::App::new()
            .app_data(web::Data::new(pm_clone.clone()))
            .service(handlers::write_timeseries)
            .service(handlers::query_timeseries)
            .service(handlers::list_timeseries)
            .service(handlers::query_timeseries_range)
    })
    .bind(&http_addr)
    .unwrap()
    .run();

    // Start UDP server
    let udp_addr_str = "127.0.0.1:8089".to_string();
    let pm_clone = pm.clone();
    let _udp_server = UDPRefluxServer::new(udp_addr_str, pm_clone).await;

    // Spawn HTTP server in background
    actix_rt::spawn(server);

    // Give servers time to start
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    (format!("http://{}", http_addr), pm)
}

#[actix_rt::test]
async fn test_end_to_end() {
    // Keep temp_file alive for the entire test
    let temp_file = NamedTempFile::new().unwrap();
    let db_path = temp_file.path().to_str().unwrap();
    let pm = Arc::new(Mutex::new(
        TimeseriesPersistenceManager::new(db_path).unwrap(),
    ));

    let http_addr = "127.0.0.1:8080";
    let http_url = format!("http://{}", http_addr);
    let pm_clone = pm.clone();

    // Start HTTP server in background
    let server = actix_web::HttpServer::new(move || {
        actix_web::App::new()
            .app_data(web::Data::new(pm_clone.clone()))
            .service(handlers::write_timeseries)
            .service(handlers::query_timeseries)
            .service(handlers::list_timeseries)
            .service(handlers::query_timeseries_range)
    })
    .bind(http_addr)
    .unwrap()
    .run();

    // Start UDP server
    let udp_addr_str = "127.0.0.1:8089".to_string();
    let pm_clone = pm.clone();
    let _udp_server = UDPRefluxServer::new(udp_addr_str, pm_clone).await;

    // Spawn HTTP server in background
    actix_rt::spawn(server);

    // Give servers time to start
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Create test data
    let test_data = vec![
        "cpu,host=server1,region=us-west value=42.5 1556813561098000000",
        "cpu,host=server1,region=us-west value=43.1 1556813561098000001",
        "cpu,host=server2,region=us-east value=41.8 1556813561098000002",
        "memory,host=server1,region=us-west value=1024 1556813561098000003",
        "memory,host=server2,region=us-east value=2048 1556813561098000004",
    ];

    // Send test data via UDP
    let udp_socket = UdpSocket::bind("127.0.0.1:0").unwrap();
    for line in test_data {
        udp_socket
            .send_to(line.as_bytes(), "127.0.0.1:8089")
            .unwrap();
        thread::sleep(Duration::from_millis(10)); // Small delay between sends
    }

    // Wait for data to be processed
    thread::sleep(Duration::from_millis(100));

    // Test 1: List timeseries
    let client = reqwest::Client::new();
    let response = client.get(&format!("{}/", http_url)).send().await.unwrap();
    assert_eq!(response.status(), 200);
    let text = response.text().await.unwrap();
    // Remove quotes and parse as JSON array
    let text = text.trim_matches('"');
    let timeseries: Vec<String> = serde_json::from_str(text).unwrap();
    assert_eq!(timeseries.len(), 2);
    assert!(timeseries.contains(&"cpu".to_string()));
    assert!(timeseries.contains(&"memory".to_string()));

    // Test 2: Query specific timeseries
    let response = client
        .get(&format!("{}/query", http_url))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let text = response.text().await.unwrap();
    // Remove quotes and parse as JSON array
    let text = text.trim_matches('"');
    let data: serde_json::Value = serde_json::from_str(text).unwrap();
    assert_eq!(data.as_array().unwrap().len(), 2);

    // Test 3: Query with time range
    let response = client
        .get(&format!(
            "{}/range/cpu?start=1556813561098000000&end=1556813561098000001",
            http_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let text = response.text().await.unwrap();
    // Remove quotes and parse as JSON array
    let text = text.trim_matches('"');
    let data: serde_json::Value = serde_json::from_str(text).unwrap();
    assert_eq!(data.as_array().unwrap().len(), 2);

    // Test 4: Query with tags
    let response = client
        .get(&format!("{}/query?q=cpu", http_url))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let text = response.text().await.unwrap();
    // Remove quotes and parse as JSON array
    let text = text.trim_matches('"');
    let data: serde_json::Value = serde_json::from_str(text).unwrap();
    assert_eq!(data.as_array().unwrap().len(), 2);

    // Test 5: Write data via HTTP
    let write_data = json!({
        "measurement": "cpu",
        "tags": {
            "host": "server3",
            "region": "eu-central"
        },
        "fields": {
            "value": 44.2
        },
        "timestamp": "1556813561098000005"
    });

    let response = client
        .post(&format!("{}/write", http_url))
        .json(&write_data)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);

    // Verify the write
    let response = client
        .get(&format!("{}/query?q=cpu", http_url))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let text = response.text().await.unwrap();
    // Remove quotes and parse as JSON array
    let text = text.trim_matches('"');
    let data: serde_json::Value = serde_json::from_str(text).unwrap();
    assert_eq!(data.as_array().unwrap().len(), 1);
}

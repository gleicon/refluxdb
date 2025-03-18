use crate::persistence::TimeseriesPersistenceManager;
use crate::protocol::LineProtocol;
use crate::udpserver::UDPRefluxServer;
use std::net::UdpSocket;
use std::sync::{Arc, Mutex};
use tempfile::NamedTempFile;
use tokio::runtime::Runtime;

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    async fn setup_test_server() -> (NamedTempFile, UDPRefluxServer) {
        let temp_file = NamedTempFile::new().unwrap();
        let db_path = temp_file.path().to_str().unwrap();
        let pm = Arc::new(Mutex::new(
            TimeseriesPersistenceManager::new(db_path).unwrap(),
        ));

        // Use a random available port
        let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = socket.local_addr().unwrap().to_string();
        drop(socket); // Release the socket so the server can bind to it

        let server = UDPRefluxServer::new(addr, pm).await;
        (temp_file, server)
    }

    #[tokio::test]
    async fn test_single_measurement() {
        let rt = Runtime::new().unwrap();
        let (_temp_file, mut server) = rt.block_on(setup_test_server());
        let addr = server.socket.local_addr().unwrap();
        let pm = server.get_persistence_manager();

        // Start server in a separate thread
        let server_thread = thread::spawn(move || {
            let rt = Runtime::new().unwrap();
            rt.block_on(server.run(false)).unwrap();
        });

        // Give the server time to start
        thread::sleep(Duration::from_millis(100));

        // Create a test client
        let client = UdpSocket::bind("127.0.0.1:0").unwrap();

        // Send a test measurement
        let test_data = "test_measurement,tag1=value1 fieldKey=\"42.0\" 1556813561098000000\n";
        client.send_to(test_data.as_bytes(), addr).unwrap();

        // Give the server time to process
        thread::sleep(Duration::from_millis(100));

        // Verify the measurement was stored
        let measurements = {
            let pm = pm.lock().unwrap();
            pm.get_measurement_range("test_measurement", 0, i64::MAX)
                .await
        }
        .unwrap();
        assert_eq!(measurements.len(), 1);
        assert_eq!(measurements[0].1, 42.0);

        // Clean up
        drop(server_thread);
    }

    #[tokio::test]
    async fn test_multiple_measurements() {
        let rt = Runtime::new().unwrap();
        let (_temp_file, mut server) = rt.block_on(setup_test_server());
        let addr = server.socket.local_addr().unwrap();
        let pm = server.get_persistence_manager();

        let server_thread = thread::spawn(move || {
            let rt = Runtime::new().unwrap();
            rt.block_on(server.run(false)).unwrap();
        });

        thread::sleep(Duration::from_millis(100));

        let client = UdpSocket::bind("127.0.0.1:0").unwrap();

        // Send multiple measurements
        let test_data = "test_measurement,tag1=value1 fieldKey1=\"42.0\",fieldKey2=\"43.0\" 1556813561098000000\n";
        client.send_to(test_data.as_bytes(), addr).unwrap();

        thread::sleep(Duration::from_millis(100));

        // Verify both measurements were stored
        let measurements = {
            let pm = pm.lock().unwrap();
            pm.get_measurement_range("test_measurement", 0, i64::MAX)
                .await
        }
        .unwrap();
        assert_eq!(measurements.len(), 2);
        assert!(measurements.iter().any(|m| m.1 == 42.0));
        assert!(measurements.iter().any(|m| m.1 == 43.0));

        // Clean up
        drop(server_thread);
    }

    #[tokio::test]
    async fn test_invalid_protocol() {
        let rt = Runtime::new().unwrap();
        let (_temp_file, mut server) = rt.block_on(setup_test_server());
        let addr = server.socket.local_addr().unwrap();
        let pm = server.get_persistence_manager();

        let server_thread = thread::spawn(move || {
            let rt = Runtime::new().unwrap();
            rt.block_on(server.run(false)).unwrap();
        });

        thread::sleep(Duration::from_millis(100));

        let client = UdpSocket::bind("127.0.0.1:0").unwrap();

        // Send invalid protocol data
        let invalid_data = "invalid protocol data\n";
        client.send_to(invalid_data.as_bytes(), addr).unwrap();

        thread::sleep(Duration::from_millis(100));

        // Verify no measurements were stored
        let measurements = {
            let pm = pm.lock().unwrap();
            pm.get_measurement_range("test_measurement", 0, i64::MAX)
                .await
        }
        .unwrap();
        assert_eq!(measurements.len(), 0);

        // Clean up
        drop(server_thread);
    }

    #[tokio::test]
    async fn test_concurrent_clients() {
        let rt = Runtime::new().unwrap();
        let (_temp_file, mut server) = rt.block_on(setup_test_server());
        let addr = server.socket.local_addr().unwrap();
        let pm = server.get_persistence_manager();

        let server_thread = thread::spawn(move || {
            let rt = Runtime::new().unwrap();
            rt.block_on(server.run(false)).unwrap();
        });

        thread::sleep(Duration::from_millis(100));

        let mut handles = vec![];

        // Spawn 10 client threads
        for i in 0..10 {
            let addr = addr;
            let handle = thread::spawn(move || {
                let client = UdpSocket::bind("127.0.0.1:0").unwrap();
                let test_data = format!(
                    "test_measurement,tag1=value1 fieldKey=\"{}\" 1556813561098000000\n",
                    i
                );
                client.send_to(test_data.as_bytes(), addr).unwrap();
            });
            handles.push(handle);
        }

        // Wait for all clients to complete
        for handle in handles {
            handle.join().unwrap();
        }

        thread::sleep(Duration::from_millis(100));

        // Verify all measurements were stored
        let measurements = {
            let pm = pm.lock().unwrap();
            pm.get_measurement_range("test_measurement", 0, i64::MAX)
                .await
        }
        .unwrap();
        assert_eq!(measurements.len(), 10);

        // Clean up
        drop(server_thread);
    }
}

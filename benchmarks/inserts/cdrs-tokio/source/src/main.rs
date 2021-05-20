use std::sync::Arc;
use std::error::Error;
use cdrs_tokio::authenticators::NoneAuthenticator;
use cdrs_tokio::cluster::session::{new as new_session};
use cdrs_tokio::cluster::{ClusterTcpConfig, NodeTcpConfigBuilder};
use cdrs_tokio::load_balancing::RoundRobin;
use cdrs_tokio::query::*;
use cdrs_tokio::query_values;
use cdrs_tokio::retry::DefaultRetryPolicy;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Number of inserts to be performed
    let num_of_inserts: i32 = 10_000;

    let node = NodeTcpConfigBuilder::new("scylla:9042", Arc::new(NoneAuthenticator {})).build();
    let cluster_config = ClusterTcpConfig(vec![node]);
    let session = new_session(&cluster_config, RoundRobin::new(), Box::new(DefaultRetryPolicy::default())).await?;

    // Create a simple keyspace and table
    session
        .query(
            "CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION = \
            {'class' : 'SimpleStrategy', 'replication_factor' : 1}",
        )
        .await?;

    session
        .query(
            "CREATE TABLE IF NOT EXISTS ks.benchtab (a int primary key)",
        )
        .await?;
        
    let insert_statement = session.prepare("INSERT INTO ks.benchtab (a) VALUES(?)").await?;

    let session = Arc::new(session);
    let prepared = Arc::new(insert_statement);

    // Start the benchmark
    println!("Starting the benchmark ({} inserts)", num_of_inserts);

    let mut handles = Vec::with_capacity(num_of_inserts as usize);

    let start_time = std::time::Instant::now();

    for i in 0..num_of_inserts {
        let session = session.clone();
        let prepared = prepared.clone();
        
        let handle = tokio::spawn(async move {
            session.exec_with_values(&prepared, query_values!(i)).await.unwrap();
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.await?;
    }

    let elapsed = start_time.elapsed();
    println!("The benchmark took: {}ms", elapsed.as_millis());

    Ok(())
}
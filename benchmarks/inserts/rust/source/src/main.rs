use scylla::{Session, SessionBuilder};
use scylla::prepared_statement::PreparedStatement;
use std::sync::Arc;
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Number of inserts to be performed
    let num_of_inserts: i32 = 10_000;

    let session: Session = SessionBuilder::new()
        .known_node("scylla:9042")
        .build()
        .await?;

    // Create a simple keyspace and table
    session
        .query(
            "CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION = \
            {'class' : 'SimpleStrategy', 'replication_factor' : 1}",
            &[],
        )
        .await?;

    session
        .query(
            "CREATE TABLE IF NOT EXISTS ks.benchtab (a int primary key)",
            &[],
        )
        .await?;
    
    let insert_statement = session.prepare("INSERT INTO ks.benchtab (a) VALUES(?)").await?;

    let session: Arc<Session> = Arc::new(session);
    let prepared: Arc<PreparedStatement> = Arc::new(insert_statement);

    // Start the benchmark
    println!("Starting the benchmark ({} inserts)", num_of_inserts);

    let mut handles = Vec::with_capacity(num_of_inserts as usize);

    let start_time = std::time::Instant::now();

    for i in 0..num_of_inserts {
        let session = session.clone();
        let prepared = prepared.clone();
        
        let handle = tokio::spawn(async move {
            session.execute(&prepared, (i,)).await.unwrap();
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
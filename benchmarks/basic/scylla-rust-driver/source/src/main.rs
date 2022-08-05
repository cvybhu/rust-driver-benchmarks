mod config;

use anyhow::Result;
use config::{Config, Workload};
use scylla::prepared_statement::PreparedStatement;
use scylla::{IntoTypedRows, Session, SessionBuilder};
use std::convert::TryInto;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {
    println!("Starting scylla-rust-driver benchmark\n");

    let config: Arc<Config> = match Config::read()? {
        Some(config) => Arc::new(config),
        None => return Ok(()), // --help only prints usage
    };

    println!("Benchmark configuration:\n{:#?}\n", config);

    let session: Session = SessionBuilder::new()
        .known_nodes(&config.node_addresses)
        .build()
        .await?;

    let session = Arc::new(session);

    if !config.dont_prepare {
        prepare_keyspace_and_table(&session).await?;
    }

    let insert_stmt = "INSERT INTO benchks.benchtab (pk, v1, v2) VALUES(?, ?, ?)";
    let select_stmt = "SELECT v1, v2 FROM benchks.benchtab WHERE pk = ?";

    let prepared_insert = Arc::new(session.prepare(insert_stmt).await?);
    let prepared_select = Arc::new(session.prepare(select_stmt).await?);

    if config.workload == Workload::Selects && !config.dont_prepare {
        prepare_selects_benchmark(&session, &prepared_insert, &config).await?;
    }

    let mut handles = Vec::with_capacity(config.concurrency.try_into().unwrap());
    let next_batch_start = Arc::new(AtomicI64::new(0));

    println!("\nStarting the benchmark");

    let start_time = std::time::Instant::now();

    for _ in 0..config.concurrency {
        let session = session.clone();
        let prepared_insert = prepared_insert.clone();
        let prepared_select = prepared_select.clone();
        let config = config.clone();
        let next_batch_start = next_batch_start.clone();

        handles.push(tokio::spawn(async move {
            loop {
                let cur_batch_start: i64 =
                    next_batch_start.fetch_add(config.batch_size, Ordering::Relaxed);

                if cur_batch_start >= config.tasks {
                    // No more work to do
                    break;
                }

                let cur_batch_end: i64 =
                    std::cmp::min(cur_batch_start + config.batch_size, config.tasks);

                for pk in cur_batch_start..cur_batch_end {
                    if config.workload == Workload::Inserts || config.workload == Workload::Mixed {
                        session
                            .execute(&prepared_insert, (pk, 2 * pk, 3 * pk))
                            .await
                            .unwrap();
                    }

                    if config.workload == Workload::Selects || config.workload == Workload::Mixed {
                        let (v1, v2): (i64, i64) = session
                            .execute(&prepared_select, (pk,))
                            .await
                            .unwrap()
                            .rows
                            .unwrap()
                            .into_typed::<(i64, i64)>()
                            .next()
                            .unwrap()
                            .unwrap();

                        assert_eq!((v1, v2), (2 * pk, 3 * pk));
                    }
                }
            }
        }));
    }

    for handle in handles {
        handle.await?;
    }

    let bench_time = start_time.elapsed();
    println!("Finished\n\nBenchmark time: {} ms", bench_time.as_millis());

    Ok(())
}

async fn prepare_keyspace_and_table(session: &Session) -> Result<()> {
    session
        .query("DROP KEYSPACE IF EXISTS benchks", &[])
        .await?;

    session.await_schema_agreement().await?;

    session
        .query(
            "CREATE KEYSPACE IF NOT EXISTS benchks WITH REPLICATION = \
            {'class' : 'SimpleStrategy', 'replication_factor' : 1}",
            &[],
        )
        .await?;

    session.await_schema_agreement().await?;

    session
        .query(
            "CREATE TABLE IF NOT EXISTS benchks.benchtab (pk bigint PRIMARY KEY, v1 bigint, v2 bigint)",
            &[],
        )
        .await?;

    session.await_schema_agreement().await?;

    Ok(())
}

async fn prepare_selects_benchmark(
    session: &Arc<Session>,
    prepared_insert: &Arc<PreparedStatement>,
    config: &Arc<Config>,
) -> Result<()> {
    println!("Preparing a selects benchmark (inserting values)...");

    let mut handles = Vec::with_capacity(config.concurrency.try_into().unwrap());
    let next_batch_start = Arc::new(AtomicI64::new(0));

    for _ in 0..std::cmp::max(1024, config.concurrency) {
        let session = session.clone();
        let prepared_insert = prepared_insert.clone();
        let config = config.clone();
        let next_batch_start = next_batch_start.clone();

        handles.push(tokio::spawn(async move {
            loop {
                let cur_batch_start: i64 =
                    next_batch_start.fetch_add(config.batch_size, Ordering::Relaxed);

                if cur_batch_start >= config.tasks {
                    // No more work to do
                    break;
                }

                let cur_batch_end: i64 =
                    std::cmp::min(cur_batch_start + config.batch_size, config.tasks);

                for pk in cur_batch_start..cur_batch_end {
                    session
                        .execute(&prepared_insert, (pk, 2 * pk, 3 * pk))
                        .await
                        .unwrap();
                }
            }
        }));
    }

    for handle in handles {
        handle.await?;
    }

    Ok(())
}

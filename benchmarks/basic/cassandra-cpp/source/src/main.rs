mod config;

use anyhow::Result;
use cassandra_cpp::{Cluster, PreparedStatement, Session, Statement};
use config::{Config, Workload};
use std::convert::TryInto;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {
    println!("Starting cassandra-cpp benchmark\n");

    let config: Arc<Config> = match Config::read()? {
        Some(config) => Arc::new(config),
        None => return Ok(()), // --help only prints usage
    };

    println!("Benchmark configuration:\n{:#?}\n", config);

    let mut cluster = Cluster::default();

    for node_address in &config.node_addresses {
        cluster.set_contact_points(&node_address).unwrap();
    }

    cluster.set_queue_size_io(std::cmp::max(2048, (2 * config.concurrency).try_into().unwrap())).unwrap();
    cluster.set_num_threads_io(num_cpus::get().try_into().unwrap()).unwrap();

    let session: Arc<Session> = Arc::new(cluster.connect_async().await.unwrap());

    if !config.dont_prepare {
        prepare_keyspace_and_table(&session).await?;
    }

    let insert_stmt = "INSERT INTO benchks.benchtab (pk, v1, v2) VALUES(?, ?, ?)";
    let select_stmt = "SELECT v1, v2 FROM benchks.benchtab WHERE pk = ?";

    let prepared_insert = session.prepare(insert_stmt).unwrap().await.unwrap();
    let prepared_select = session.prepare(select_stmt).unwrap().await.unwrap();

    if config.workload == Workload::Selects && !config.dont_prepare {
        prepare_selects_benchmark(&session, &prepared_insert, &config).await?;
    }

    let mut handles = Vec::with_capacity(config.concurrency.try_into().unwrap());
    let next_batch_start = Arc::new(AtomicI64::new(0));

    println!("\nStarting the benchmark");

    let start_time = std::time::Instant::now();

    for _ in 0..config.concurrency {
        let session = session.clone();
        let config = config.clone();
        let next_batch_start = next_batch_start.clone();
        let mut insert_stmt = prepared_insert.bind();
        let mut select_stmt = prepared_select.bind();

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
                        insert_stmt.bind_int64(0, pk).unwrap();
                        insert_stmt.bind_int64(1, 2 * pk).unwrap();
                        insert_stmt.bind_int64(2, 3 * pk).unwrap();

                        let fut = session.execute(&insert_stmt);
                        fut.await.unwrap();
                    }

                    if config.workload == Workload::Selects || config.workload == Workload::Mixed {
                        select_stmt.bind_int64(0, pk).unwrap();

                        let fut = session.execute(&select_stmt);
                        let res = fut.await.unwrap();
                        let first_row = res.first_row().unwrap();

                        let v1: i64 = first_row.get_column(0).unwrap().get_i64().unwrap();
                        let v2: i64 = first_row.get_column(1).unwrap().get_i64().unwrap();

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
    let drop_ks = Statement::new("DROP KEYSPACE IF EXISTS benchks", 0);

    session.execute(&drop_ks).await.unwrap();

    let create_ks = Statement::new(
        "CREATE KEYSPACE IF NOT EXISTS benchks WITH REPLICATION = \
    {'class' : 'SimpleStrategy', 'replication_factor' : 1}",
        0,
    );

    session.execute(&create_ks).await.unwrap();

    let create_table = Statement::new(
        "CREATE TABLE IF NOT EXISTS benchks.benchtab (pk bigint PRIMARY KEY, v1 bigint, v2 bigint)",
        0,
    );

    session.execute(&create_table).await.unwrap();

    Ok(())
}

async fn prepare_selects_benchmark(
    session: &Arc<Session>,
    prepared_insert: &PreparedStatement,
    config: &Arc<Config>,
) -> Result<()> {
    println!("Preparing a selects benchmark (inserting values)...");

    let mut handles = Vec::with_capacity(config.concurrency.try_into().unwrap());
    let next_batch_start = Arc::new(AtomicI64::new(0));

    for _ in 0..std::cmp::max(1024, config.concurrency) {
        let session = session.clone();
        let mut insert_stmt = prepared_insert.bind();
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
                    insert_stmt.bind_int64(0, pk).unwrap();
                    insert_stmt.bind_int64(1, 2 * pk).unwrap();
                    insert_stmt.bind_int64(2, 3 * pk).unwrap();

                    let fut = session.execute(&insert_stmt);
                    fut.await.unwrap();
                }
            }
        }));
    }

    for handle in handles {
        handle.await?;
    }

    Ok(())
}

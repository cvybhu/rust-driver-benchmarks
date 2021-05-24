use anyhow::{anyhow, Result};
use getopts::Options;
use scylla::prepared_statement::PreparedStatement;
use scylla::{IntoTypedRows, Session, SessionBuilder};
use std::convert::TryInto;
use std::sync::Arc;
use tokio::sync::Semaphore;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Workload {
    Inserts,
    Selects,
    Mixed,
}

#[derive(Debug)]
struct Config {
    node_address: String,
    workload: Workload,
    tasks: i64,
    concurrency: i64,
    no_prepare: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("Starting scylla-rust-driver benchmark\n");

    let config: Config = match read_config()? {
        Some(config) => config,
        None => return Ok(()),
    };

    println!("Benchmark configuration:\n{:#?}\n", config);

    let session: Session = SessionBuilder::new()
        .known_node(config.node_address.clone())
        .build()
        .await?;

    let session = Arc::new(session);

    if !config.no_prepare {
        prepare_keyspace_and_table(&session).await?;
    }

    let insert_stmt = "INSERT INTO benchks.benchtab (pk, v1, v2) VALUES(?, ?, ?)";
    let select_stmt = "SELECT v1, v2 FROM benchks.benchtab WHERE pk = ?";

    let prepared_insert = Arc::new(session.prepare(insert_stmt).await?);
    let prepared_select = Arc::new(session.prepare(select_stmt).await?);

    if config.workload == Workload::Selects && !config.no_prepare {
        prepare_selects_benchmark(&session, &prepared_insert, &config).await?;
    }

    let concurrency_semaphore = Arc::new(Semaphore::new(config.concurrency.try_into().unwrap()));

    let mut i: i64 = 0;
    let batch_size = 256;

    let mut last_progress_report_time = std::time::Instant::now();

    println!("\nStarting the benchmark");

    let start_time = std::time::Instant::now();

    while i < config.tasks {
        if last_progress_report_time.elapsed() > std::time::Duration::from_secs(1) {
            let progress: f32 = i as f32 / config.tasks as f32 * 100.0;
            println!("Progress: {:.1}%", progress);
            last_progress_report_time = std::time::Instant::now();
        }

        let session = session.clone();
        let prepared_insert = prepared_insert.clone();
        let prepared_select = prepared_select.clone();
        let workload = config.workload;

        let concurrency_permit = concurrency_semaphore.clone().acquire_owned().await;

        let begin: i64 = i;
        let end: i64 = std::cmp::min(begin + batch_size, config.tasks);

        tokio::task::spawn(async move {
            for j in begin..end {
                if workload == Workload::Inserts || workload == Workload::Mixed {
                    session
                        .execute(&prepared_insert, (j, 2 * j, 3 * j))
                        .await
                        .unwrap();
                }

                if workload == Workload::Selects || workload == Workload::Mixed {
                    let (v1, v2) = session
                        .execute(&prepared_select, (j,))
                        .await
                        .unwrap()
                        .rows
                        .unwrap()
                        .into_typed::<(i64, i64)>()
                        .next()
                        .unwrap()
                        .unwrap();

                    assert_eq!((v1, v2), (2 * j, 3 * j));
                }
            }

            std::mem::drop(concurrency_permit);
        });

        i += batch_size;
    }

    for _ in 0..config.concurrency {
        concurrency_semaphore.acquire().await?.forget();
    }

    let bench_time = start_time.elapsed();
    println!("Finished\n\nBenchmark time: {} ms", bench_time.as_millis());

    Ok(())
}

async fn prepare_keyspace_and_table(session: &Session) -> Result<()> {
    session
        .query("DROP KEYSPACE IF EXISTS benchks", &[])
        .await?;

    session
        .query(
            "CREATE KEYSPACE IF NOT EXISTS benchks WITH REPLICATION = \
            {'class' : 'SimpleStrategy', 'replication_factor' : 1}",
            &[],
        )
        .await?;

    session
        .query(
            "CREATE TABLE IF NOT EXISTS benchks.benchtab (pk bigint PRIMARY KEY, v1 bigint, v2 bigint)",
            &[],
        )
        .await?;

    Ok(())
}

async fn prepare_selects_benchmark(
    session: &Arc<Session>,
    prepared_insert: &Arc<PreparedStatement>,
    config: &Config,
) -> Result<()> {
    let concurrency_semaphore = Arc::new(Semaphore::new(config.concurrency.try_into().unwrap()));

    let mut i: i64 = 0;
    let batch_size = 256;

    let mut last_progress_report_time = std::time::Instant::now();

    println!("Preparing a selects benchmark (inserting values)...");

    while i < config.tasks {
        if last_progress_report_time.elapsed() > std::time::Duration::from_secs(1) {
            let progress: f32 = i as f32 / config.tasks as f32 * 100.0;
            println!("Preparing progress: {:.1}%", progress);
            last_progress_report_time = std::time::Instant::now();
        }

        let session = session.clone();
        let prepared_insert = prepared_insert.clone();

        let concurrency_permit = concurrency_semaphore.clone().acquire_owned().await;

        let begin: i64 = i;
        let end: i64 = begin + batch_size;

        tokio::task::spawn(async move {
            for j in begin..end {
                session
                    .execute(&prepared_insert, (j, 2 * j, 3 * j))
                    .await
                    .unwrap();
            }

            std::mem::drop(concurrency_permit);
        });

        i += batch_size;
    }

    for _ in 0..config.concurrency {
        concurrency_semaphore.acquire().await?.forget();
    }

    Ok(())
}

fn read_config() -> Result<Option<Config>> {
    let mut opts = Options::new();

    opts.optflag("h", "help", "Print usage information");

    opts.optopt(
        "a",
        "address",
        "Address of database node to connect to
        (default: 'scylla:9042')",
        "ADDRESS",
    );
    opts.optopt(
        "w",
        "workload",
        "Type of work to perform (inserts, selects, mixed) (default: mixed)",
        "WORKLOAD",
    );
    opts.optopt("t", "tasks", "Total number of tasks (requests) to perform the during benchmark
                               In case of mixed workload there will be tasks/2 inserts and tasks/2 selects
                               (default: 1 000 000)", "TASKS");
    opts.optopt(
        "c",
        "concurrency",
        "Maximum number of requests performed at once
        (default: 256)",
        "CONCURRENCY",
    );

    opts.optflag(
        "n",
        "no-prepare",
        "Don't crate tables and insert into them before the benchmark",
    );

    let args: Vec<String> = std::env::args().collect();
    let parsed = opts.parse(&args[1..])?;

    if parsed.opt_present("help") {
        print_usage(&opts);
        return Ok(None);
    }

    let address: String = parsed.opt_get_default("address", "scylla:9042".to_string())?;

    let workload_str: String = parsed.opt_get_default("workload", "mixed".to_string())?;
    let workload: Workload = match workload_str.as_str() {
        "inserts" => Workload::Inserts,
        "selects" => Workload::Selects,
        "mixed" => Workload::Mixed,
        other => {
            return Err(anyhow!(
                "Invalid workload type: '{}'. Allowed values: inserts, selects, mixed",
                other
            ))
        }
    };

    let tasks: i64 = parsed.opt_get_default("tasks", 1_000_000)?;
    let concurrency: i64 = parsed.opt_get_default("concurrency", 256)?;

    let no_prepare: bool = parsed.opt_present("no-prepare");

    Ok(Some(Config {
        node_address: address,
        workload,
        tasks,
        concurrency,
        no_prepare,
    }))
}

fn print_usage(opts: &Options) {
    println!("{}", opts.usage("Benchmark configuration"));
}

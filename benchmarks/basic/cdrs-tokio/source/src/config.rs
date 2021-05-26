use anyhow::{anyhow, Result};
use getopts::Options;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Workload {
    Inserts,
    Selects,
    Mixed,
}

#[derive(Debug)]
pub struct Config {
    pub node_address: String,
    pub workload: Workload,
    pub tasks: i64,
    pub concurrency: i64,
    pub batch_size: i64,
    pub no_prepare: bool,
}

impl Config {
    pub fn read() -> Result<Option<Config>> {
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
                                In case of mixed workload there will be tasks inserts and tasks selects
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
            println!("{}", opts.usage("Benchmark configuration"));
            return Ok(None);
        }

        let address: String = parsed.opt_get_default("address", "scylla:9042".to_string())?;

        let workload_str: String = parsed.opt_get_default("workload", "inserts".to_string())?;
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

        let mut batch_size = 256;

        if tasks / batch_size < concurrency {
            batch_size = std::cmp::max(1, tasks / concurrency);
        }

        Ok(Some(Config {
            node_address: address,
            workload,
            tasks,
            concurrency,
            batch_size,
            no_prepare,
        }))
    }
}

# Basic benchmark
This benchmark measures performance of basic operations (insert, select) on a single scylla node.

## Options
This benchmark can be configured using command line arguments passed to `run.sh`:

* `-a`, `--address` - Address of the node where the driver will connect.  
Note that `rust` and `cdrs-tokio` require a port
while `cpp-driver` takes just the ip address and assumes the port is `9042`  
(default: `"scylla:9042"`/`"scylla"`)
* `-w`, `--workload` - Type of task to perform
    * `inserts` - Insert a new row into the table
    * `selects` - Select a single row from the table
    * `mixed` - First insert a new row and then select it

    (default: mixed)
* `-t`, `--tasks` - Total number of tasks to perform (in case of `mixed` insert and select is a single task)  
(default: 1 000 000)
* `-c`, `--concurrency` - Maximum number of requests performed at once  
(default: 256)
* `-n`, `--no-prepare` - Don't create the keyspace and table (and insert values in case of `selects` workload)  
Normally when the `selects` workload is used the driver will crate a keyspace, table and insert the values to select.
When this option is disabled the benchmark assumes this has already been done. This can be achieved by first running
the driver with `inserts` workload.  
(Not enabled by default)
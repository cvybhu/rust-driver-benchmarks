# Basic benchmark
This benchmark measures performance of basic operations (insert, select) on a single scylla node.

## Options
This benchmark can be configured using command line arguments passed to `run.sh`:

* `-n`, `--nodes` - Addresses of database nodes to connect to separated by a comma.
Note that `rust` and `cdrs-tokio` require a port
while `cpp-driver` and `cassandra-cpp` take just the ip address and assume the port is `9042`  
(default: `"127.0.0.1:9042"`/`"127.0.0.1"`)
* `-w`, `--workload` - Type of task to perform
    * `inserts` - Insert a new row into the table
    * `selects` - Select a single row from the table
    * `mixed` - First insert a new row and then select it

    (default: mixed)
* `-t`, `--tasks` - Total number of tasks to perform (in case of `mixed` insert + select is a single task)  
(default: 1 000 000)
* `-c`, `--concurrency` - Maximum number of requests performed at once  
(default: 1024)
* `-d`, `--dont-prepare` - Don't create the keyspace and table (and don't insert values in case of `selects` workload)  
Normally when the `selects` workload is used the driver will create a keyspace, table and insert the values to select.
When this option is disabled the benchmark assumes this has already been done. This can be achieved by first running
the driver with `inserts` workload.  
(Not enabled by default)

Abbreviated versions of command line arguments do not work with `gocql` benchmark.

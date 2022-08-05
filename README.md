# Rust driver benchmarks
Benchmarks for Scylla Rust Driver

## Benchmarks
* Basic - performs basic operations (inserts/selects), max `concurrency` active requests at once  
![](images/bench10to7-inserts.svg)

## Drivers:
* `scylla-rust-driver` - [Scylla Rust driver](https://github.com/scylladb/scylla-rust-driver)
* `cpp` - [Scylla C++ driver](https://github.com/scylladb/cpp-driver)
* `cpp-multi`- [Datastax Cassandra C++ Driver](https://github.com/datastax/cpp-driver), running on multiple threads.
* `cassandra-cpp` - [Rust bindings for Datastax C++ Driver](https://github.com/datastax/cpp-driver), running on multiple threads using Tokio.
* `gocql` - A [driver written in Go](https://github.com/gocql/gocql)
* `cdrs-tokio` - Another compatible [driver written in Rust](https://github.com/krojew/cdrs-tokio)

## How to run a benchmark
Each benchmark is inside a Docker image  
Build the image and run the benchmark  
If you're using docker executing the commands might require `sudo` or being in the docker group.

For example to run the `basic` benchmark using `scylla-rust-driver`
* Start a local scylla instance: `./scylla.sh`
* Enter benchmark directory: `cd benchmarks/basic/rust`
* Build the benchmark: `./build.sh`
* Wait until scylla is ready (Should say something like `listening on 172.17.0.3`)
* Run the benchmark: `./run.sh`

## How to generate a result chart
Chart generator is also in a docker image for convenience

* Enter the `generate_chart` directory
* Build the docker image: `./build.sh`
* Enter benchmark results in `config.py`
* Generate the chart: `./generate.sh`
* Generated chart will be in `chart.png`

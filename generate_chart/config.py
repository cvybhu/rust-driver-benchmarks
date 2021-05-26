# Title displayed at the top of the generated chart
benchmark_title = "1 000 000 inserts, concurrency = 256"

# Benchmark results in ms
benchmark_results = {
    'scylla-rust-driver': 4330,
    'cpp-driver': 4645,
    'cdrs-tokio': 17098,
    'gocql': 12548
}

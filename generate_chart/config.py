# Title displayed at the top of the generated chart
benchmark_title = "1 000 000 inserts, concurrency = 1024"

# Benchmark results in ms
benchmark_results = {
    'scylla-rust-driver': 3734,
    'cpp-driver': 5510,
    'cdrs-tokio': 17840,
    'gocql': 14802
}

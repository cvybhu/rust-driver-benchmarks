# This script helps estimate how long a series of benchmarks would take

import os

task_options = [1000 * 1000, 10 * 1000 * 1000]
concurrency_options = [64, 128, 256, 512, 1024, 2048, 4096, 8192]
workflow_options = ["inserts", "selects", "mixed"]

drivers = ["rust", "cpp", "cpp-multi", "cassandra-cpp", "cdrs-tokio", "gocql"]

restarts = False

# Each configuration is repeated samples times to ensure consistent results
samples = 3

configurations = []
for t in task_options:
    for c in concurrency_options:
        for w in workflow_options:
            if t > 1000 * 1000 and c < 512:
                continue

            configurations.append((t, c, w))

# Returns estimated time in seconds to complete single sample of a benchmark using rust driver
def estimate_time(configuration):
    t = configuration[0] / configuration[1]

    # Scale so that it matches observed time
    t = t * 0.005

    # Take into account the time needed to prepare a selects benchmark
    if configuration[2] == "selects" or configuration[2] == "mixed":
        t += estimate_time((configuration[0], max(1024, configuration[1]), "inserts"))

    return t

# How much time each driver takes compared to scylla-rust-driver
rust_coef = 1
cpp_coef = 5510/3734
cdrs_tokio_coef = 17840/3734
gocql_coef = 14802/3734
cassandra_rs_coef = cpp_coef

total_coef = rust_coef + cpp_coef + cdrs_tokio_coef + gocql_coef + cassandra_rs_coef


total_time = sum([estimate_time(c) * total_coef * samples for c in configurations])

if restarts:
    # For each configuration and sample each driver does a 4 minute restart
    total_time += len(configurations) * samples * 5 * 4 * 60

print(f"Number of configurations: {len(configurations)}")
print(f"total_time: {total_time / 60 / 60} hours")
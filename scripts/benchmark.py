import subprocess
import sys

task_options = [1000 * 1000, 10 * 1000 * 1000]
concurrency_options = [64, 128, 256, 512, 1024, 2048, 4096, 8192]
workload_options = ["inserts", "selects", "mixed"]

drivers = ["rust", "cpp", "cassandra-rs", "gocql", "cdrs-tokio"]

# Each configuration is repeated samples times to ensure consistent results
samples = 3

nodes_no_ports = ["scylla"]
#nodes_no_ports = ["10.0.0.226", "10.0.1.52", "10.0.2.86"]

nodes_with_ports = [n + ":9042" for n in nodes_no_ports]

configurations = []
for t in task_options:
    for c in concurrency_options:
        for w in workload_options:
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

sorted_confs = sorted(configurations, key = lambda c : estimate_time(c))

for (tasks, concurrency, workload) in sorted_confs:
    for driver in drivers:
        for sample in range(samples):
            # Run the benchmark
            nodes = ""
            if driver in ["cpp", "cassandra-rs"]:
                nodes = ",".join(nodes_no_ports)
            else:
                nodes = ",".join(nodes_with_ports)

            print(f'\nBENCHMARK {{"driver": "{driver}", "workload": "{workload}", "concurrency": {concurrency}, "tasks": {tasks}, "sample": {sample}}}')
            sys.stdout.flush()

            subprocess.call([f"../benchmarks/basic/{driver}/run.sh", 
                             "--nodes", nodes, 
                             "--workload", workload,
                             "--concurrency", str(concurrency),
                             "--tasks", str(tasks)])


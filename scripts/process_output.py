import sys
import json

s = sys.stdin.read()
benchmarks = s.split("BENCHMARK")

for benchmark in benchmarks:
    line_with_time_pattern = "Benchmark time: "
    time_pos = benchmark.find(line_with_time_pattern)
    if time_pos == -1:
        continue

    time_number_pos = time_pos + len(line_with_time_pattern)
    time = int(benchmark[time_number_pos:].split(" ")[0])

    benchmark_json = json.loads(benchmark.split("\n")[0])
    benchmark_json["time"] = time

    print(json.dumps(benchmark_json))


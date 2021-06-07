#!/bin/bash
docker run --rm -it --link scylla rust-driver-benchmarks-basic-cassandra-cpp /source/basic "$@"
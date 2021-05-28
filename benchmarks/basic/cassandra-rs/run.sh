#!/bin/bash
docker run --rm -it --link scylla rust-driver-benchmarks-basic-cassandra-rs /source/basic "$@"
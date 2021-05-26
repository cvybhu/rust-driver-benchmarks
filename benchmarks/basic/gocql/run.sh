#!/bin/bash
docker run --rm -it --link scylla rust-driver-benchmarks-basic-gocql /source/basic "$@"

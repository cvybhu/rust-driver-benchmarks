#!/bin/bash
docker run --rm -it --link scylla rust-driver-benchmarks-basic-cdrs-tokio /source/basic "$@"
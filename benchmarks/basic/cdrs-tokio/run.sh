#!/bin/bash
docker run --rm -it --network host rust-driver-benchmarks-basic-cdrs-tokio /source/basic "$@"
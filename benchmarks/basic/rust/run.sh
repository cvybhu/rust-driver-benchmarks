#!/bin/bash
docker run --rm -it --network host rust-driver-benchmarks-basic-rust /source/basic "$@"

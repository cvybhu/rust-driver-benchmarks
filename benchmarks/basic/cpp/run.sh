#!/bin/bash
docker run --rm -it --network host rust-driver-benchmarks-basic-cpp /source/basic "$@"
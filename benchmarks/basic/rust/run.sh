#!/bin/bash
docker run --rm -it --link scylla rust-driver-benchmarks-basic-rust /source/target/release/basic "$@"

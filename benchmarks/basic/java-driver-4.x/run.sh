#!/bin/bash
docker run --rm -it --network host rust-driver-benchmarks-basic-java-driver-4.x \
java -cp /source/target/source-1.0-SNAPSHOT.jar MainClass "$@"

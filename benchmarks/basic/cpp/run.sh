#!/bin/bash
CONTAINER_TOOL="podman"

if ! [ -x "$(command -v podman)" ]; then
    CONTAINER_TOOL="docker"
fi

$CONTAINER_TOOL run --rm -it --network host rust-driver-benchmarks-basic-cpp /source/basic "$@"
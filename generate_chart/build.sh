#!/bin/bash
CONTAINER_TOOL="podman"

if ! [ -x "$(command -v podman)" ]; then
    CONTAINER_TOOL="docker"
fi

$CONTAINER_TOOL docker build . -t rust-driver-benchmarks-generate-chart
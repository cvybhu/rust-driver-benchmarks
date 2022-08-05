#!/bin/bash
CONTAINER_TOOL="podman"

if ! [ -x "$(command -v podman)" ]; then
    CONTAINER_TOOL="docker"
fi

$CONTAINER_TOOL build "$@" . -t rust-driver-benchmarks-basic-rust
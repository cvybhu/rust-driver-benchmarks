#!/bin/bash
touch chart.png

CONTAINER_TOOL="podman"

if ! [ -x "$(command -v podman)" ]; then
    CONTAINER_TOOL="docker"
fi

$CONTAINER_TOOL run --rm -it \
    -v $(pwd)/config.py:/source/config.py:ro \
    -v $(pwd)/chart.png:/source/chart.png \
    rust-driver-benchmarks-generate-chart python3 generate_chart.py
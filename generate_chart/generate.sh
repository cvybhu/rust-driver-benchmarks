#!/bin/bash
touch chart.png

docker run --rm -it \
    -v $(pwd)/config.py:/source/config.py:ro \
    -v $(pwd)/chart.png:/source/chart.png \
    rust-driver-benchmarks-generate-chart python3 generate_chart.py
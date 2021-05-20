#!/bin/bash
touch generated.png

docker run --rm -it -v $(pwd)/generated.png:/source/generated.png rust-driver-benchmarks-generate-chart python3 generate_chart.py
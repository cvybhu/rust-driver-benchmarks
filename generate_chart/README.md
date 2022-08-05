## How to generate a result chart
Chart generator is also in a docker image for convenience

* Enter the `generate_chart` directory
* Build the docker image: `./build.sh`
* Enter benchmark results in `config.py`
* Generate the chart: `./generate.sh`
* Generated chart will be in `chart.png`
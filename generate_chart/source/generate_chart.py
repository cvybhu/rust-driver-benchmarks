import matplotlib.pyplot as plt
import config

driver_names = []
driver_results = []

for driver_name in config.benchmark_results:
    driver_names.append(driver_name)

for driver_result in config.benchmark_results.values():
    driver_results.append(driver_result)


colors = ['red', 'blue', 'teal', 'green', 'yellow']

plt.bar(driver_names, driver_results, color = colors[:len(driver_names)])
plt.title(config.benchmark_title)
plt.xlabel('Driver')
plt.ylabel('Time in milliseconds (less is better)')

# TODO: Show number above bars

plt.savefig('chart.png')
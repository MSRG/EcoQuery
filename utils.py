import csv
import json
import math
import os
import time
from collections import namedtuple

import numpy as np
import psutil
import pyRAPL
import requests
import ipinfo
from matplotlib import pyplot as plt

from core.tracker import Tracker

from core.rapl_config import rapl_reader

carbon_intensity = 0

LinuxDiskIOStats = namedtuple('DiskIOStats',
                              ['read_count', 'write_count', 'read_bytes', 'write_bytes', 'read_time', 'write_time',
                               'read_merged_count', 'write_merged_count', 'busy_time'])


def print_tracker_results(job, tracker_results):
    print("Results for " + job + ":")
    if tracker_results['dram_total_energy'] != 0:
        print("Total energy consumed by RAM is:" + str(tracker_results['dram_total_energy']) + " J")
        print("Average energy consumed by RAM is:" + str(tracker_results['avg_dram_energy']) + " J/sec")
        print("Total carbon emitted by RAM is:" + str(
            calculate_kwh(tracker_results['avg_dram_energy']) * carbon_intensity) + " gCO₂eq")
        print(
            "Total energy consumed by CPU is " + str(
                tracker_results['total_cpu_energy']) + " J")
        print("Average energy consumed by CPU is " + str(
            tracker_results['avg_cpu_energy']) + " J/sec")
        print("Total carbon emitted by CPU is " + str(
            calculate_kwh(tracker_results['total_cpu_energy']) * carbon_intensity) + " gCO₂eq")
    for i in range(len(tracker_results['cpu_total_energy_per_Soc'])):
        print(
            "Total energy consumed by package " + str(i) + " is " + str(
                tracker_results['cpu_total_energy_per_Soc'][i]) + " J")
        print("Average energy consumed by package " + str(i) + " is " + str(
            tracker_results['avg_cpu_energy_per_Soc'][i]) + " J/sec")
        print("Total carbon emitted by package " + str(i) + " is " + str(
            calculate_kwh(tracker_results['cpu_total_energy_per_Soc'][i]) * carbon_intensity) + " gCO₂eq")
    print("Total execution time is: " + str(tracker_results['total_duration']) + " sec")


def get_ip():
    response = requests.get('https://api64.ipify.org?format=json').json()
    return response["ip"]


def get_location():
    ip_address = get_ip()
    access_token = '23d3ea0900771d'
    handler = ipinfo.getHandler(access_token)
    details = handler.getDetails(ip_address)
    location_data = {
        "ip": ip_address,
        "city": details.city,
        # "region": response.get("region"),
        "country": details.country,
        "latitude": details.latitude,
        "longitude": details.longitude
    }

    return location_data


def get_latest_carbon_intensity():
    location = get_location()
    print(location)
    latitude = location.get("latitude")
    longitude = location.get("longitude")
    url = "https://api-access.electricitymaps.com/free-tier/carbon-intensity/latest?lat=" + str(
        latitude) + "&lon=" + str(longitude)
    headers = {
        "auth-token": "9oKv71Ozq9kcZSOw9UrFK8UZL9ahMNXM"
    }
    response = requests.get(url, headers=headers)
    print(response.text)
    data = json.loads(response.text)
    global carbon_intensity
    carbon_intensity = data.get("carbonIntensity")
    # return data


def calculate_kwh(energy_uJ):
    power_kwh = energy_uJ / (3.6 * 10 ** 12)  # From μJoules to kWh
    return power_kwh


def initializeOutput(benchmark, database):
    csv_header = ['Label', 'dram_avgE(J/sec)', 'dram_totalE(J)', 'dram_carbon(gCO₂eq)', 'cpu_avgE(J/sec)',
                  'cpu_totalE(J)', 'cpu_carbon(gCO₂eq)', ]
    if not os.path.exists(benchmark.path + "results/csv/"):
        # Create the directory
        os.makedirs(benchmark.path + "results/csv/")
    with open(benchmark.path + "results/csv/" + benchmark.type + "_" + benchmark.sf + "_" + database + ".csv", 'w',
              encoding='UTF8') as f:
        cpu_counter = 0
        for package in rapl_reader.packages:
            if 'package' not in package.name:
                continue
            csv_header.append('cpu_' + str(cpu_counter) + '_avgE(J/sec)')
            csv_header.append('cpu_' + str(cpu_counter) + '_totalE(J)')
            csv_header.append('cpu_' + str(cpu_counter) + '_carbon(gCO₂eq)')
            cpu_counter = cpu_counter + 1
        csv_header.append('Latency(sec)')
        # IO headers
        csv_header.append('Device')
        csv_header.append('Read Count')
        csv_header.append('Write Count')
        csv_header.append('Read Bytes')
        csv_header.append('Write Bytes')
        csv_header.append('Read Time(ms)')
        csv_header.append('Write Time(ms)')
        csv_header.append('Busy Time(ms)')
        csv_header.append('cpu_utilization(%)')
        csv_header.append('dram_usage(GB)')
        csv_header.append('dram_cache(GB)')
        csv_header.append('dram_total(GB)')

        writer = csv.writer(f)
        # write the header
        writer.writerow(csv_header)


def export_query_stats(benchmark, database, label, results, latency, io_counters):
    data = [label]
    if not os.path.exists(benchmark.path + "results/csv/"):
        # Create the directory
        os.makedirs(benchmark.path + "results/csv/")
    with open(benchmark.path + "results/csv/" + benchmark.type + "_" + benchmark.sf + "_" + database + ".csv", 'a',
              encoding='UTF8') as f:
        writer = csv.writer(f)
        data.append(results['avg_dram_energy'])
        data.append(results['dram_total_energy'])
        data.append(calculate_kwh(results['dram_total_energy']) * carbon_intensity)
        data.append(results['avg_cpu_energy'])
        data.append(results['total_cpu_energy'])
        data.append(calculate_kwh(results['total_cpu_energy']) * carbon_intensity)
        cpu_counter = 0
        for package in rapl_reader.packages:
            if 'package' not in package.name:
                continue
            data.append(results['avg_cpu_energy_per_Soc'][cpu_counter])
            data.append(results['cpu_total_energy_per_Soc'][cpu_counter])
            data.append(calculate_kwh(results['cpu_total_energy_per_Soc'][cpu_counter]) * carbon_intensity)
            cpu_counter = cpu_counter + 1
        data.append(latency)
        if len(io_counters.items()) == 0:
            data.append("None")
            data.append(0)
            data.append(0)
            data.append(0)
            data.append(0)
            data.append(0)
            data.append(0)
            data.append(0)
        else:
            devices = []
            read_count = []
            write_count = []
            read_bytes = []
            write_bytes = []
            read_time = []
            write_time = []
            busy_time = []

            for device, io_counter in io_counters.items():
                if 'loop' in device:
                    continue
                devices.append(device)
                read_count.append(io_counter.read_count)
                write_count.append(io_counter.write_count)
                read_bytes.append(io_counter.read_bytes)
                write_bytes.append(io_counter.write_bytes)
                read_time.append(io_counter.read_time)
                write_time.append(io_counter.write_time)
                busy_time.append(io_counter.busy_time)

            data.append(devices)
            data.append(read_count)
            data.append(write_count)
            data.append(read_bytes)
            data.append(write_bytes)
            data.append(read_time)
            data.append(write_time)
            data.append(busy_time)

        data.append(results['cpu_usage'])
        data.append(results['mem_usage'])
        data.append(results['mem_cache'])
        data.append(results['total_mem'])
        writer.writerow(data)


def export_throughput(benchmark, database, throughput):
    csv_header = ['Throughput(queries/sec)']
    if not os.path.exists(benchmark.path + "results/csv/"):
        # Create the directory
        os.makedirs(benchmark.path + "results/csv/")
    with open(benchmark.path + "results/csv/" + benchmark.type + "_" + benchmark.sf + "_" + database + ".csv", 'a',
              encoding='UTF8') as f:
        writer = csv.writer(f)
        # write the header
        writer.writerow(csv_header)
        data = [throughput]
        writer.writerow(data)


def measure_io_operations(initial_io_counters, final_io_counters):
    # Calculate the difference in IO counters
    io_counters_diff = {}
    for device, final_counter in final_io_counters.items():
        initial_counter = initial_io_counters.get(device)
        diff_counter = LinuxDiskIOStats(
            read_count=final_counter.read_count - initial_counter.read_count,
            write_count=final_counter.write_count - initial_counter.write_count,
            read_bytes=final_counter.read_bytes - initial_counter.read_bytes,
            write_bytes=final_counter.write_bytes - initial_counter.write_bytes,
            read_time=final_counter.read_time - initial_counter.read_time,
            write_time=final_counter.write_time - initial_counter.write_time,
            read_merged_count=final_counter.read_merged_count - initial_counter.read_merged_count,
            write_merged_count=final_counter.write_merged_count - initial_counter.write_merged_count,
            busy_time=final_counter.busy_time - initial_counter.busy_time
        )
        io_counters_diff[device] = diff_counter

    return io_counters_diff


def filter_non_zero_counters(io_counters_diff):
    # Filter out devices with no IO operations
    non_zero_counters = {device: counter for device, counter in io_counters_diff.items() if any(counter)}
    return non_zero_counters


def plotMemoryUsage(memory_cache, memory_usage_gb, benchmark, database, job):
    x_axis = range(len(memory_usage_gb))
    # Plot the memory utilization
    # plt.plot(x_axis, memory_utilization, label='Memory Utilization (%)')

    #memory_usage_gb = [round(value, 3) for value in memory_usage_gb]

    # Plot the actual gigabytes of occupied memory
    plt.plot(x_axis, memory_usage_gb, label='Memory Usage (GB)')

    # Set labels and title
    plt.xlabel('Measurements')
    plt.ylabel('Memory Usage')
    plt.title('Memory Usage Over Time - ' + job)

    # Add legend
    plt.legend()

    # Calculate the step size based on the number of measurements
    step_size = max(1, 10 ** (math.ceil(math.log10(len(memory_usage_gb))) - 1))

    # Set x-axis ticks to display every 10th label
    plt.xticks(x_axis[::step_size])

    # plt.show(block=False)

    # Check if the directory exists
    if not os.path.exists(benchmark.path + "results/figures/"):
        # Create the directory
        os.makedirs(benchmark.path + "results/figures/")
    plt.savefig(
        benchmark.path + "results/figures/" + benchmark.type + "_" + benchmark.sf + "_" + database + "_" + job + "_dram" + ".png")
    # Clear the current figure for the next plot
    plt.clf()
    # Explicitly close the figure to avoid accumulating open figures
    plt.close()

    # Plot the actual gigabytes of occupied memory
    plt.plot(x_axis, memory_cache, label='Memory Cache (GB)')

    # Set labels and title
    plt.xlabel('Measurements')
    plt.ylabel('Memory Cache')
    plt.title('Memory Cache Over Time - ' + job)

    # Add legend
    plt.legend()

    # Calculate the step size based on the number of measurements
    step_size = max(1, 10 ** (math.ceil(math.log10(len(memory_usage_gb))) - 1))

    # Set x-axis ticks to only include integers
    # Set x-axis ticks to display every 10th label
    plt.xticks(x_axis[::step_size])

    # Check if the directory exists
    if not os.path.exists(benchmark.path + "results/figures/"):
        # Create the directory
        os.makedirs(benchmark.path + "results/figures/")
    plt.savefig(
        benchmark.path + "results/figures/" + benchmark.type + "_" + benchmark.sf + "_" + database + "_" + job + "_cache" + ".png")
    # Clear the current figure for the next plot
    plt.clf()
    # Explicitly close the figure to avoid accumulating open figures
    plt.close()


def plotCPUUsage(cpu_utilization, benchmark, database, job):
    # Generate thread indices
    thread_indices = np.arange(len(cpu_utilization))

    # Create a bar plot
    plt.figure(figsize=(10, 6))  # Adjust figure size as needed
    plt.bar(thread_indices, cpu_utilization, color='skyblue')

    # Add labels and title
    plt.xlabel('CPU Thread')
    plt.ylabel('Average CPU Utilization')
    plt.title('Average CPU Utilization Per Thread - ' + job)

    # Add value labels on top of each bar
    for i, util in enumerate(cpu_utilization):
        plt.text(i, util, str(round(util, 2)), ha='center', va='bottom')

    plt.grid(axis='y')  # Show grid on y-axis
    plt.xticks(thread_indices)  # Set x ticks to match thread indices
    plt.tight_layout()  # Adjust layout

    # Check if the directory exists
    if not os.path.exists(benchmark.path + "results/figures/"):
        # Create the directory
        os.makedirs(benchmark.path + "results/figures/")
    plt.savefig(
        benchmark.path + "results/figures/" + benchmark.type + "_" + benchmark.sf + "_" + database + "_" + job + "_cpu" + ".png")
    # Clear the current figure for the next plot
    plt.clf()
    plt.close()


def calculate_run_stats(benchmark, database, job, tracker_results, io_counters_start, io_counters_end, latency):
    # Measure IO operations
    io_operations_result = measure_io_operations(io_counters_start, io_counters_end)

    # Filter out disk devices with no IO operations
    non_zero_counters = filter_non_zero_counters(io_operations_result)

    print_tracker_results(job, tracker_results)
    export_query_stats(benchmark, database, job, tracker_results, latency, non_zero_counters)
    plotMemoryUsage(tracker_results["mem_cache"], tracker_results["mem_usage"], benchmark, database, job)
    plotCPUUsage(tracker_results["cpu_usage"], benchmark, database, job)


# Extract scale factor from data path
def extract_sf_from_path(path):
    # Get the base name of the path
    base_name = os.path.basename(path)
    # Extract scale factor
    parts = base_name.split('_')
    return parts[1]


# Calculates the average energy consumption before executing anything
def calculate_idle_average_energy(duration, benchmark, db_type):
    print("Measuring the energy consumption of the system in idle state for " + str(duration) + " seconds\n")
    tracker = Tracker(100000, "machine", "avgEnergy")
    start_time = time.time()
    tracker.start()
    io_counters_start = psutil.disk_io_counters(perdisk=True)
    while time.time() - start_time < duration:
        # Do nothing
        pass
    tracker.stop()
    io_counters_end = psutil.disk_io_counters(perdisk=True)
    print_tracker_results("idle state", tracker.results)
    data = ["idle state"]
    if not os.path.exists(benchmark.path + "results/csv/"):
        # Create the directory
        os.makedirs(benchmark.path + "results/csv/")
    with open(benchmark.path + "results/csv/" + benchmark.type + "_" + benchmark.sf + "_" + db_type + ".csv", 'a',
              encoding='UTF8') as f:
        writer = csv.writer(f)
        data.append(tracker.results['avg_dram_energy'])
        data.append(tracker.results['dram_total_energy'])
        data.append(calculate_kwh(tracker.results['dram_total_energy']) * carbon_intensity)
        data.append(tracker.results['avg_cpu_energy'])
        data.append(tracker.results['total_cpu_energy'])
        data.append(calculate_kwh(tracker.results['total_cpu_energy']) * carbon_intensity)

        cpu_counter = 0
        for package in rapl_reader.packages:
            if 'package' not in package.name:
                continue
            data.append(tracker.results['avg_cpu_energy_per_Soc'][cpu_counter])
            data.append(tracker.results['cpu_total_energy_per_Soc'][cpu_counter])
            data.append(calculate_kwh(tracker.results['cpu_total_energy_per_Soc'][cpu_counter]) * carbon_intensity)
            cpu_counter = cpu_counter + 1
        data.append(duration)
        writer.writerow(data)

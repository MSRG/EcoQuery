import time

import psutil
import pyRAPL
import pyRAPL.outputs
from pyRAPL import Measurement
from threading import Timer, Lock
import matplotlib.pyplot as plt

from core.rapl_config import rapl_reader
from periodic import PeriodicMeter
from core.measurement import EnergyMeasurement


class Tracker(object):
    def __init__(self, interval, benchmark, measurement):
        self.interval = interval
        self.benchmark = benchmark
        self.meter = pyRAPL.Measurement(measurement)

        self.energy_meas = EnergyMeasurement(rapl_reader)

        self.last_measured_time: float = 0
        self.cpu_total_energy_per_Soc = []  # Total energy consumption per CPU socket
        self.avgCPUEnergy_per_Soc = []
        self.cpu_total_energy = 0  # Total energy consumption of the CPU
        self.avgCPUEnergy = 0
        for package in rapl_reader.packages:
            if 'package' not in package.name:
                continue
            self.cpu_total_energy_per_Soc.append(0)
            self.avgCPUEnergy_per_Soc.append(0)

        # for i in range(len(pyRAPL._sensor._socket_ids)):
        #    self.cpu_total_energy.append(0)
        #    self.avgCPUEnergy.append(0)

        self.dram_total_energy = 0
        self.total_duration = 0
        self.avgRAMEnergy = 0

        self._scheduler = PeriodicMeter(
            function=self.measureEnergy,
            interval=self.interval
        )

        self.results = {}

        # Initialize memory utilization tracking
        self.memory_utilization = []
        self.memory_usage_gb = []  # Used memory over time
        self.memory_cache_gb = []  # Cached memory over time
        self.total_memory_gb = []  # Total memory over time
        self.cpu_utilization = []  # Average CPU Utilization per thread
        self.lock = Lock()

    def start(self):
        # self.meter.begin()
        self.last_measured_time = time.time()
        self._scheduler.start()

        self.energy_meas.begin()

        # Get memory usage information
        memory = psutil.virtual_memory()
        used_percentage = memory.percent
        used_gb = memory.used / (1024 ** 3)  # Convert bytes to gigabytes
        cached_gb = (memory.cached + memory.buffers) / (1024 ** 3)  # Memory for cache/buffers
        total_mem = used_gb + cached_gb

        # Append the new data point
        with self.lock:
            self.memory_utilization.append(used_percentage)
            self.memory_usage_gb.append(used_gb)
            self.memory_cache_gb.append(cached_gb)
            self.total_memory_gb.append(total_mem)

        cpu_util = psutil.cpu_percent(interval=None, percpu=True)  # Initialize the tool

    def stop(self):
        time_difference = time.time() - self.last_measured_time
        # Wait for the RAPL registers to update
        if time_difference * 1000 < 1.6:
            sleep_duration = 1.6 / 1000 - time_difference
            time.sleep(sleep_duration)
        # self.meter.end()
        self.energy_meas.end()
        self._scheduler.stop()
        cpu_util = psutil.cpu_percent(interval=None, percpu=True)
        # print(str(cpu_util) + " stop")

        measurement_duration = self.energy_meas.time_end - self.energy_meas.time_begin
        self.total_duration = self.total_duration + measurement_duration  # Seconds
        cpu_counter = 0
        for package_name, package_data in self.energy_meas.result.items():
            if 'package' not in package_name:
                continue
            self.cpu_total_energy_per_Soc[cpu_counter] = self.cpu_total_energy_per_Soc[cpu_counter] + (
                    package_data['energy'] / 1e6)  # From μJ to J
            self.avgCPUEnergy_per_Soc[cpu_counter] = self.cpu_total_energy_per_Soc[cpu_counter] / self.total_duration
            self.cpu_total_energy = self.cpu_total_energy + (package_data['energy'] / 1e6)
            self.avgCPUEnergy = self.avgCPUEnergy + self.avgCPUEnergy_per_Soc[cpu_counter]

            # Iterate over subzones for the current package
            for subzone_name, subzone_energy_value in package_data['subzones'].items():
                # Add the energy of the current DRAM DIMM
                if 'dram' in subzone_name:
                    self.dram_total_energy = self.dram_total_energy + (subzone_energy_value / 1e6)
            self.avgRAMEnergy = self.dram_total_energy / self.total_duration
            cpu_counter = cpu_counter + 1

        # result = self.meter.result
        # Store results
        # self.total_duration = self.total_duration + result.duration
        # if result.dram is not None:
        # self.dram_total_energy = self.dram_total_energy + float(result.dram)  # / float(result.duration)
        # self.avgRAMEnergy = self.dram_total_energy / self.total_duration
        # print("Energy consumed by RAM is:" + str(float(result.dram) / float(result.duration)) + " μJ/μsec")
        # for i in range(len(pyRAPL._sensor._socket_ids)):
        # self.avgCPUEnergy[i] = self.cpu_total_energy[i] / self.total_duration
        # print("Energy consumed by package " + str(i) + " is " + str(
        #   float(result.pkg[i]) / float(result.duration)) + " μJ/μsec")
        self.results['avg_cpu_energy_per_Soc'] = self.avgCPUEnergy_per_Soc
        self.results['avg_dram_energy'] = self.avgRAMEnergy
        self.results['cpu_total_energy_per_Soc'] = self.cpu_total_energy_per_Soc
        self.results['dram_total_energy'] = self.dram_total_energy
        self.results['total_duration'] = self.total_duration
        self.results['total_cpu_energy'] = self.cpu_total_energy
        self.results['avg_cpu_energy'] = self.avgCPUEnergy

        # Get memory usage information
        memory = psutil.virtual_memory()
        used_percentage = memory.percent
        used_gb = memory.used / (1024 ** 3)  # Convert bytes to gigabytes
        cached_gb = (memory.cached + memory.buffers) / (1024 ** 3)  # Memory for cache/buffers
        total_mem = used_gb + cached_gb  # Total memory consumption

        # Append the new data point
        with self.lock:
            self.memory_utilization.append(used_percentage)
            self.memory_usage_gb.append(used_gb)
            self.memory_cache_gb.append(cached_gb)
            self.total_memory_gb.append(total_mem)
            self.cpu_utilization.extend(cpu_util)

        self.results["mem_utilization"] = self.memory_utilization
        self.results["mem_usage"] = [round(value, 2) for value in self.memory_usage_gb]
        self.results["cpu_usage"] = self.cpu_utilization
        self.results["mem_cache"] = [round(value, 2) for value in self.memory_cache_gb]
        self.results["total_mem"] = [round(value, 2) for value in self.total_memory_gb]

    def measureEnergy(self):
        # self.meter.end()

        self.energy_meas.end()
        measurement_duration = self.energy_meas.time_end - self.energy_meas.time_begin
        self.total_duration = self.total_duration + measurement_duration
        cpu_counter = 0
        for package_name, package_data in self.energy_meas.result.items():
            if 'package' not in package_name:
                continue
            self.cpu_total_energy_per_Soc[cpu_counter] = self.cpu_total_energy_per_Soc[cpu_counter] + (
                    package_data['energy'] / 1e6)
            self.avgCPUEnergy_per_Soc[cpu_counter] = self.cpu_total_energy_per_Soc[cpu_counter] / self.total_duration
            self.cpu_total_energy = self.cpu_total_energy + (package_data['energy'] / 1e6)

            # Iterate over subzones for the current package
            for subzone_name, subzone_energy_value in package_data['subzones'].items():
                if 'dram' in subzone_name:
                    self.dram_total_energy = self.dram_total_energy + (subzone_energy_value / 1e6)
            cpu_counter = cpu_counter + 1

        # result = self.meter.result
        # if result.dram is not None and float(result.dram) > 0:
        #   self.dram_total_energy = self.dram_total_energy + float(result.dram) / float(result.duration)
        # print("Energy consumed by RAM is:" + str(float(result.dram) / float(result.duration)) + " μJ/μsec")
        # for i in range(len(pyRAPL._sensor._socket_ids)):
        #   if float(result.pkg[i]) > 0:
        #       self.cpu_total_energy[i] = self.cpu_total_energy[i] + float(result.pkg[i]) / float(result.duration)
        # print("Energy consumed by package " + str(i) + " is " + str(
        #   float(result.pkg[i]) / float(result.duration)) + " μJ/μsec")
        # self.total_duration = self.total_duration + result.duration

        # self.meter.begin()

        # Start a measurement for the next interval
        self.energy_meas.begin()

        # Get memory usage information
        memory = psutil.virtual_memory()
        used_percentage = memory.percent
        used_gb = memory.used / (1024 ** 3)  # Convert bytes to gigabytes
        cached_gb = (memory.cached + memory.buffers) / (1024 ** 3)  # Memory for cache/buffers
        total_mem = used_gb + cached_gb

        # Append the new data point
        with self.lock:
            self.memory_utilization.append(used_percentage)
            self.memory_usage_gb.append(used_gb)
            self.memory_cache_gb.append(cached_gb)
            self.total_memory_gb.append(total_mem)
            # self.cpu_utilization.append(cpu_util)

import os
import time


class EnergyMeasurement:
    def __init__(self, rapl_reader, rapl_path="/sys/class/powercap/intel-rapl"):
        self.rapl_path = rapl_path
        self.rapl_reader = rapl_reader
        self.energy_begin = None
        self.time_begin = None
        self.energy_end = None
        self.time_end = None
        self.result = None

    def begin(self):
        self.time_begin = time.time()
        self.energy_begin = self.measure_energy_consumption()

    def end(self):
        self.time_end = time.time()
        self.energy_end = self.measure_energy_consumption()

        self.result = {}
        for package_name, package_data_begin in self.energy_begin.items():
            self.result[package_name] = {
                'energy': max(0, self.energy_end[package_name]['energy'] - package_data_begin['energy']),
                # Check if the rapl counter has reset
                'subzones': {}
            }
            for subzone_name, subzone_energy_begin in package_data_begin['subzones'].items():
                self.result[package_name]['subzones'][subzone_name] = (
                    max(0, self.energy_end[package_name]['subzones'][subzone_name] - subzone_energy_begin)
                )

    def measure_energy_consumption(self):
        energy_data = {}

        for package in self.rapl_reader.packages:
            package.measure_energy_consumption()
            energy_data[package.name] = {
                'energy': package.energy_uj,
                'subzones': {}
            }
            for subzone in package.subzones:
                subzone.measure_energy_consumption()
                energy_data[package.name]['subzones'][subzone.name] = subzone.energy_uj
        return energy_data

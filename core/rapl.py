import os

from core.measurement import EnergyMeasurement


def is_subzone_folder(folder_name):
    # Check if the folder name follows the pattern intel-rapl:{zone number}:{subzone number}
    parts = folder_name.split(':')
    return len(parts) == 3 and parts[0] == 'intel-rapl' and all(part.isdigit() for part in parts[1:])


class Subzone:
    def __init__(self, subzone_path,name, zone, subzone):
        self.name = name
        self.zone = zone
        self.subzone = subzone
        self.energy_uj = None
        self.subzone_path = subzone_path
        self.energy_file_path = os.path.join(self.subzone_path, "energy_uj")
        if not os.path.exists(self.energy_file_path):
            raise Exception(f"Energy file not found for {self.name} subzone.")

    def measure_energy_consumption(self):
        with open(self.energy_file_path, "r") as energy_file:
            self.energy_uj = int(energy_file.read().strip())


class Package:
    def __init__(self, package_path, name):
        self.name = name
        self.subzones = []
        self.energy_uj = None
        self.package_path = package_path
        self.dram = None
        self.energy_file_path = os.path.join(self.package_path, "energy_uj")
        if not os.path.exists(self.energy_file_path):
            raise Exception(f"Energy file not found for {self.name} package.")

    def create_subzones(self):
        # self.subzones = [Subzone(name, zone, subzone) for zone, subzone in subzones]

        # Filter contents that are not folders
        subzone_folders = [subzone for subzone in os.listdir(self.package_path) if
                           os.path.isdir(os.path.join(self.package_path, subzone))]
        # Extract zone and subzone numbers
        for subzone_folder in subzone_folders:
            if is_subzone_folder(subzone_folder):
                _, zone, subzone = subzone_folder.split(':')
                subzone_path = os.path.join(self.package_path, subzone_folder)
                subzone_name_file_path = os.path.join(self.package_path, subzone_folder, "name")
                if os.path.exists(subzone_name_file_path):
                    with open(subzone_name_file_path, "r") as name_file:
                        name = name_file.read().strip()
                        subzone = Subzone(subzone_path,name, zone, subzone)
                        if "dram" in name:
                            self.dram = subzone
                        self.subzones.append(subzone)
                else:
                    raise Exception(f"{self.name} - Subzones: {subzone} - Name file not found.")

    def measure_energy_consumption(self):
        # Measure energy consumption for the entire package
        with open(self.energy_file_path, "r") as energy_file:
            self.energy_uj = int(energy_file.read().strip())

        # Measure energy consumption for each subzone
        for subzone in self.subzones:
            subzone.measure_energy_consumption()


class RAPLReader:

    def __init__(self):
        self.rapl_path = "/sys/class/powercap/intel-rapl"
        self.packages = []

    def setup(self):
        self.create_packages()
        for package in self.packages:
            package.create_subzones()

    def create_packages(self):
        packages = [zone for zone in os.listdir(self.rapl_path) if zone.startswith("intel-rapl")]
        for package in packages:
            package_path = os.path.join(self.rapl_path, package)
            name_file_path = os.path.join(self.rapl_path, package, "name")
            if os.path.exists(name_file_path):
                with open(name_file_path, "r") as name_file:
                    name = name_file.read().strip()
                    print(f"{package} - Name: {name}")
                    self.packages.append(Package(package_path, name))
            else:
                print(f"{package} - Name file not found.")
        print()


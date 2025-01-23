import glob
import math
import os
import re

import seaborn as sns
import matplotlib
import pandas as pd
from matplotlib import pyplot as plt
matplotlib.use('Qt5Agg')

# List of databases
databases = ['DuckDB', 'Hyper', 'Starrocks', 'MonetDB']

average_data = {}  # Holds the average of all experiments of the average values of different metrics
total_data = {} # Holds the average  of all experiments of the total sum values of different metrics

ontario_carbon_intensity = 85
ontario_energy_mix = {
    "Biomass": 0.09 / 100,
    "Wind": 6.85 / 100,
    "Nuclear": 58.96 / 100,
    "Natural gas": 15.48 / 100,
    "Hydropower": 18.62 / 100
}
scale_factor = 1 # No scale factor here

# Liters/Joule
water_consumption_intensity = {
    "Biomass": 0.000000504722,
    "Hydropower": 0.0000143,
    "Nuclear": 0.000000611111,
    "Oil": 0.000000485,
    "Coal and lignite": 0.000000504722,
    "Geothermal": 0.000000378611,
    "Natural gas": 0.00000019444,
    "Solar": 0.0000000125,
    "Wind": 0.000000000513889
}

hardware_total_embodied_carbon ={ # gCO2e
    "HDD": 7110,
    #"SSD": 13670.4,
    "CPU": 4740.92 # total emissions
}

dram_embodied_carbon_per_gb = 63 # gCO2e/GB

hardware_configuration = ["HDD","CPU","DRAM"]

metric_labels = {
    'number_of_queries': {
        'title': '',
        'y_label': 'Queries until capex and opex equal - Log scale'
    },
    'days': {
        'title': '',
        'y_label': 'Days until capex and opex equal'
    }
}

def convert_to_gb(size_string):
    # Remove 'GB' and convert to integer
    return int(size_string.replace('GB', ''))

def extract_all_columns(df):
    return df.columns.tolist()

def aggregate_metrics(directory, dram_size):
    columns_to_process = ['dram_totalE(J)', 'cpu_totalE(J)','Latency(sec)']

    for db in databases:
        # Initialize dictionary for the database if not already present
        if db not in average_data:
            average_data[db] = {}
            total_data[db] = {}
        if dram_size not in average_data[db]:
            average_data[db][dram_size] = {}
            total_data[db][dram_size] = {}
        # Glob pattern for finding CSV files in the directory
        files = glob.glob(os.path.join(directory, "results/csv/*.csv"))
        # Loop through each file
        for file in files:
            filename = file.split('/')[-1]
            config = filename.split('_')
            if db.lower() not in config[2].lower():
                continue
            # Read CSV file
            df = pd.read_csv(file, delimiter=',')

            df_filtered = df.iloc[:-2]  # Exclude the last two rows
            df_load = df_filtered.iloc[1]
            df_filtered = df_filtered.iloc[2:]  # Exclude the first two rows

            # Get all unique columns dynamically
            all_columns = extract_all_columns(df_filtered)
            # Average values of selected metrics for all the queries' benchmark
            average_queries_values = df_filtered[columns_to_process].mean()

            for metric, avg_value in average_queries_values.items():
                if metric not in average_data[db][dram_size]:
                    average_data[db][dram_size][metric] = 0
                    average_data[db][dram_size][metric] = average_data[db][dram_size][metric] + avg_value
                average_data[db][dram_size][metric] = (average_data[db][dram_size][
                                                           metric] + avg_value) / 2  # Running average of average metric for all experiments
            # Total values of selected metrics for all the queries' benchmark
            sum_queries_values = df_filtered[columns_to_process].sum()

            for metric, sum_value in sum_queries_values.items():
                if metric not in total_data[db][dram_size]:
                    total_data[db][dram_size][metric] = 0
                    total_data[db][dram_size][metric] = total_data[db][dram_size][metric] + sum_value
                total_data[db][dram_size][metric] = (total_data[db][dram_size][
                                                         metric] + sum_value) / 2  # Running average of total metric for all experiments



def calculate_carbon():
    for database, dram_sizes in average_data.items():
        for dram_size in dram_sizes.keys():
            average_data[database][dram_size]['dram_carbon(gCO₂eq)'] = (average_data[database][dram_size][
                                                                            'dram_totalE(J)'] * scale_factor / (
                                                                                3.6 * 10 ** 6)) * ontario_carbon_intensity
            average_data[database][dram_size]['cpu_carbon(gCO₂eq)'] = (average_data[database][dram_size][
                                                                           'cpu_totalE(J)'] * scale_factor / (
                                                                               3.6 * 10 ** 6)) * ontario_carbon_intensity

    for database, dram_sizes in total_data.items():
        for dram_size in dram_sizes.keys():
            total_data[database][dram_size]['dram_carbon(gCO₂eq)'] = (total_data[database][dram_size][
                                                                          'dram_totalE(J)'] * scale_factor / (
                                                                              3.6 * 10 ** 6)) * ontario_carbon_intensity
            total_data[database][dram_size]['cpu_carbon(gCO₂eq)'] = (total_data[database][dram_size][
                                                                         'cpu_totalE(J)'] * scale_factor / (
                                                                             3.6 * 10 ** 6)) * ontario_carbon_intensity






def calculate_water():
    for database, dram_sizes in total_data.items():
        for dram_size in dram_sizes.keys():
            dram_water = 0
            cpu_water = 0
            dram_water_sources = {}
            cpu_water_sources = {}
            for energy_source, water_footprint in ontario_energy_mix.items():
                dram_water = dram_water + water_footprint * total_data[database][dram_size]['dram_totalE(J)'] * \
                             water_consumption_intensity[energy_source] * scale_factor
                cpu_water = cpu_water + water_footprint * total_data[database][dram_size]['cpu_totalE(J)'] * \
                            water_consumption_intensity[energy_source] * scale_factor
                dram_water_sources[energy_source] = water_footprint * total_data[database][dram_size][
                    'dram_totalE(J)'] * \
                                                    water_consumption_intensity[energy_source] * scale_factor
                cpu_water_sources[energy_source] = water_footprint * total_data[database][dram_size]['cpu_totalE(J)'] * \
                                                   water_consumption_intensity[energy_source] * scale_factor
            total_data[database][dram_size]['dram_water(L)'] = dram_water
            total_data[database][dram_size]['cpu_water(L)'] = cpu_water
            total_data[database][dram_size]['dram_water_sources'] = dram_water_sources
            total_data[database][dram_size]['cpu_water_sources'] = cpu_water_sources



    for database, dram_sizes in average_data.items():
        for dram_size in dram_sizes.keys():
            dram_water = 0
            cpu_water = 0
            dram_water_sources = {}
            cpu_water_sources = {}
            for energy_source, water_footprint in ontario_energy_mix.items():
                dram_water = dram_water + water_footprint * average_data[database][dram_size]['dram_totalE(J)'] * \
                             water_consumption_intensity[energy_source] * scale_factor
                cpu_water = cpu_water + water_footprint * average_data[database][dram_size]['cpu_totalE(J)'] * \
                            water_consumption_intensity[energy_source] * scale_factor
                dram_water_sources[energy_source] = water_footprint * average_data[database][dram_size][
                    'dram_totalE(J)'] * \
                                                    water_consumption_intensity[energy_source] * scale_factor
                cpu_water_sources[energy_source] = water_footprint * average_data[database][dram_size][
                    'cpu_totalE(J)'] * \
                                                   water_consumption_intensity[energy_source] * scale_factor
            average_data[database][dram_size]['dram_water(L)'] = dram_water
            average_data[database][dram_size]['cpu_water(L)'] = cpu_water
            average_data[database][dram_size]['dram_water_sources'] = dram_water_sources
            average_data[database][dram_size]['cpu_water_sources'] = cpu_water_sources



def calculate_capex_opex_relation():
    for database, dram_sizes in average_data.items():
        for dram_size in dram_sizes.keys():
            total_embodied = 0
            number_of_queries = 0  # Number of queries until equal capex and opex
            queries_per_day = 0  # Days until equal capex and opex emissions
            for component in hardware_configuration:

                component_embodied_carbon = 0
                if component == "DRAM":
                    component_embodied_carbon = dram_embodied_carbon_per_gb*convert_to_gb(dram_size)
                else:
                    component_embodied_carbon = hardware_total_embodied_carbon[component]
                total_embodied = total_embodied + component_embodied_carbon

            total_average_query_opex = average_data[database][dram_size]['dram_carbon(gCO₂eq)'] + average_data[database][dram_size]['cpu_carbon(gCO₂eq)']
            number_of_queries = math.ceil(total_embodied/total_average_query_opex)
            queries_per_day = math.ceil(24*60*60/average_data[database][dram_size]['Latency(sec)'])
            opex_per_day = queries_per_day*total_average_query_opex
            total_days = math.ceil(total_embodied/opex_per_day)
            average_data[database][dram_size]['number_of_queries'] = number_of_queries
            average_data[database][dram_size]['days'] = total_days


def plot_grouped_bars():
    average_flattened_data = []
    for db, dram_sizes in average_data.items():
        for size, metrics in dram_sizes.items():
            # Extract the numeric part of DRAM size (e.g., '96gb' -> 96)
            gb_size = int(size[:-2])
            entry = {'Database': db, 'DRAM Size': size.upper(), 'GB Size': gb_size}
            # Update the entry with metric key-value pairs
            entry.update(metrics)
            # Append the entry to the list
            average_flattened_data.append(entry)

    avg_df = pd.DataFrame(average_flattened_data)
    avg_df = avg_df.sort_values(by=['Database', 'GB Size'])

    # List of metrics to plot
    metrics_to_plot = ['number_of_queries','days']

    # Plotting each metric
    for metric in metrics_to_plot:
        plt.figure(figsize=(10, 6))
        sns.barplot(x='Database', y=metric, hue='DRAM Size', data=avg_df, errorbar=None)
        if metric == 'number_of_queries':
            plt.yscale('log')
        plt.title('')
        plt.ylabel(metric_labels[metric]['y_label'],fontsize=14 )
        plt.tick_params(axis='y', labelsize=14)
        plt.xlabel('Databases',fontsize=18, fontweight='bold',labelpad=15)
        plt.legend(title='DRAM Size', title_fontsize=16, bbox_to_anchor=(1, 1),fontsize=14,loc='best')
        plt.xticks(rotation=0,fontsize=16)
        plt.tight_layout()
        plt.savefig(f'/home/michalis/Documents/UofT/MSRG/Carbon/Artemis/Supplementary_experiments/tpch/dram/plotting/changing_dram/average_bar_{metric}_comparison.png')
        # plt.show()
        plt.close()


def main():
    base_directory = '/home/michalis/Documents/UofT/MSRG/Carbon/Artemis/Supplementary_experiments/tpch/dram/'
    for exp_folder in os.listdir(base_directory):
        directory_pattern = re.compile(r'dram_(\d+gb)_sf_\d+gb')
        match = directory_pattern.search(exp_folder)
        dram_size = ''
        if match:
            dram_size = match.group(1)
        else:
            continue
        round_path = os.path.join(base_directory, exp_folder)
        for round_folder in os.listdir(round_path):
            if round_folder.startswith("Round"):
                round_directory = os.path.join(round_path, round_folder)
                if os.path.isdir(round_directory):
                    aggregate_metrics(round_directory, dram_size.upper())

    calculate_carbon()
    calculate_water()
    calculate_capex_opex_relation()

    print(average_data)
    print(total_data)

    plot_grouped_bars()


if __name__ == "__main__":
    main()
import glob
import os
import re

import pandas as pd
import matplotlib.pyplot as plt

# List of databases
databases = ['DuckDB', 'Hyper', 'Starrocks', 'MonetDB']

average_data = {}  # Holds the average of all experiments of the average values of different metrics
total_data = {}

ontario_carbon_intensity = 85
ontario_energy_mix = {
    "Biomass": 0.09 / 100,
    "Wind": 6.85 / 100,
    "Nuclear": 58.96 / 100,
    "Natural gas": 15.48 / 100,
    "Hydropower": 18.62 / 100
}
number_of_queries = 1_000

# Liters/Joule
water_consumption = {
    "Biomass": 0.000156,
    "Hydropower": 1.51e-5,
    "Nuclear": 6.78e-7,
    "Oil": 4.95e-7,
    "Coal and lignite": 4.94e-7,
    "Geothermal": 3.4e-7,
    "Natural gas": 2.46e-7,
    "Solar": 5e-8,
    "Wind": 2e-10
}

metric_labels = {
    'dram_totalE(J)': {
        'title': 'DRAM energy consumption',
        'y_label': 'Joules'
    },
    'cpu_totalE(J)': {
        'title': 'CPU energy consumption',
        'y_label': 'Joules'
    },
    'dram_carbon(gCO₂eq)': {
        'title': 'DRAM carbon emissions',
        'y_label': 'gCO₂eq'
    },
    'cpu_carbon(gCO₂eq)': {
        'title': 'CPU carbon emissions',
        'y_label': 'gCO₂eq'
    },
    'cpu_water(L)': {
        'title': 'CPU water consumption',
        'y_label': 'Liters'
    },
    'dram_water(L)': {
        'title': 'DRAM water consumption',
        'y_label': 'Liters'
    },
    'dram_water_sources': {
        'title': 'DRAM water consumption by energy source',
        'y_label': 'Liters'
    },
    'cpu_water_sources': {
        'title': 'CPU water consumption by energy source',
        'y_label': 'Liters'
    },
    'Latency(sec)':{
        'title': 'Latency',
        'y_label': 'Seconds'
    }
}


def aggregate_metrics(directory, num_threads):
    columns_to_process = ['dram_totalE(J)', 'cpu_totalE(J)','Latency(sec)']
    for db in databases:
        # Initialize dictionary for the database if not already present
        if db not in average_data:
            average_data[db] = {}
            total_data[db] = {}
        if num_threads not in average_data[db]:
            average_data[db][num_threads] = {}
            total_data[db][num_threads] = {}
        # Glob pattern for finding CSV files in the directory
        files = glob.glob(os.path.join(directory, "csv/*.csv"))
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

            # Average values of selected metrics for all the queries' benchmark
            average_queries_values = df_filtered[columns_to_process].mean()

            for metric, avg_value in average_queries_values.items():
                if metric not in average_data[db][num_threads]:
                    average_data[db][num_threads][metric] = 0
                    average_data[db][num_threads][metric] = average_data[db][num_threads][metric] + avg_value
                average_data[db][num_threads][metric] = (average_data[db][num_threads][
                                                             metric] + avg_value) / 2  # Running average of average metric for all experiments

                # Total values of selected metrics for all the queries' benchmark
                sum_queries_values = df_filtered[columns_to_process].sum()

                for metric, sum_value in sum_queries_values.items():
                    if metric not in total_data[db][num_threads]:
                        total_data[db][num_threads][metric] = 0
                        total_data[db][num_threads][metric] = total_data[db][num_threads][metric] + sum_value
                    total_data[db][num_threads][metric] = (total_data[db][num_threads][
                                                               metric] + sum_value) / 2  # Running average of total metric for all experiments


def calculate_carbon():
    for database, thread_sizes in average_data.items():
        for num_threads in thread_sizes.keys():
            average_data[database][num_threads]['dram_carbon(gCO₂eq)'] = (average_data[database][num_threads][
                                                                              'dram_totalE(J)'] * number_of_queries / (
                                                                                  3.6 * 10 ** 6)) * ontario_carbon_intensity
            average_data[database][num_threads]['cpu_carbon(gCO₂eq)'] = (average_data[database][num_threads][
                                                                             'cpu_totalE(J)'] * number_of_queries / (
                                                                                 3.6 * 10 ** 6)) * ontario_carbon_intensity

    for database, thread_sizes in total_data.items():
        for num_threads in thread_sizes.keys():
            total_data[database][num_threads]['dram_carbon(gCO₂eq)'] = (total_data[database][num_threads][
                                                                            'dram_totalE(J)'] * number_of_queries / (
                                                                                3.6 * 10 ** 6)) * ontario_carbon_intensity
            total_data[database][num_threads]['cpu_carbon(gCO₂eq)'] = (total_data[database][num_threads][
                                                                           'cpu_totalE(J)'] * number_of_queries / (
                                                                               3.6 * 10 ** 6)) * ontario_carbon_intensity


def calculate_water():
    for database, thread_sizes in total_data.items():
        for num_threads in thread_sizes.keys():
            dram_water = 0
            cpu_water = 0
            dram_water_sources = {}
            cpu_water_sources = {}
            for energy_source, water_footprint in ontario_energy_mix.items():
                dram_water = dram_water + water_footprint * total_data[database][num_threads]['dram_totalE(J)'] * \
                             water_consumption[energy_source] * number_of_queries
                cpu_water = cpu_water + water_footprint * total_data[database][num_threads]['cpu_totalE(J)'] * \
                            water_consumption[energy_source] * number_of_queries
                dram_water_sources[energy_source] = water_footprint * total_data[database][num_threads][
                    'dram_totalE(J)'] * \
                                                    water_consumption[energy_source] * number_of_queries
                cpu_water_sources[energy_source] = water_footprint * total_data[database][num_threads]['cpu_totalE(J)'] * \
                                                   water_consumption[energy_source] * number_of_queries
            total_data[database][num_threads]['dram_water(L)'] = dram_water
            total_data[database][num_threads]['cpu_water(L)'] = cpu_water
            total_data[database][num_threads]['dram_water_sources'] = dram_water_sources
            total_data[database][num_threads]['cpu_water_sources'] = cpu_water_sources

    for database, thread_sizes in average_data.items():
        for num_threads in thread_sizes.keys():
            dram_water = 0
            cpu_water = 0
            dram_water_sources = {}
            cpu_water_sources = {}
            for energy_source, water_footprint in ontario_energy_mix.items():
                dram_water = dram_water + water_footprint * average_data[database][num_threads]['dram_totalE(J)'] * \
                             water_consumption[energy_source] * number_of_queries
                cpu_water = cpu_water + water_footprint * average_data[database][num_threads]['cpu_totalE(J)'] * \
                            water_consumption[energy_source] * number_of_queries
                dram_water_sources[energy_source] = water_footprint * average_data[database][num_threads][
                    'dram_totalE(J)'] * \
                                                    water_consumption[energy_source] * number_of_queries
                cpu_water_sources[energy_source] = water_footprint * average_data[database][num_threads][
                    'cpu_totalE(J)'] * \
                                                   water_consumption[energy_source] * number_of_queries
            average_data[database][num_threads]['dram_water(L)'] = dram_water
            average_data[database][num_threads]['cpu_water(L)'] = cpu_water
            average_data[database][num_threads]['dram_water_sources'] = dram_water_sources
            average_data[database][num_threads]['cpu_water_sources'] = cpu_water_sources


def plot_lines(data):
    # List of metrics to plot
    metrics_to_plot = ['dram_totalE(J)', 'cpu_totalE(J)', 'dram_carbon(gCO₂eq)', 'cpu_carbon(gCO₂eq)', 'dram_water(L)',
                       'cpu_water(L)','Latency(sec)']

    # Extract unique thread numbers
    threads = set()

    for database in average_data:
        threads.update(average_data[database].keys())

    # Convert the set to a sorted list if needed
    threads = sorted(threads)

    # Plotting each metric
    for metric in metrics_to_plot:
        plt.figure(figsize=(10, 6))
        for db in databases:
            values = [average_data[db].get(t, {}).get(metric, None) for t in threads]
            plt.plot(threads, values, label=db, marker='o')

        plt.title('Average ' + metric_labels[metric]['title'] + ' per query(' + str(number_of_queries) + ' executions) vs Number of threads')
        plt.xlabel('Number of Threads')
        plt.ylabel(metric_labels[metric]['y_label'] + ' - log scale')
        #plt.yscale('log')
        # Place the legend outside the plot
        plt.legend(loc='upper left', bbox_to_anchor=(1, 1))
        plt.grid(False)
        plt.xticks(sorted(threads))  # Ensure all thread counts are shown
        plt.tight_layout()
        plt.savefig(f'changing_threads/average_line_{metric}_threads.png')
        #plt.show()

    # Plotting each metric
    for metric in metrics_to_plot:
        plt.figure(figsize=(10, 6))
        for db in databases:
            values = [total_data[db].get(t, {}).get(metric, None) for t in threads]
            plt.plot(threads, values, label=db, marker='o')

        plt.title('Total ' + metric_labels[metric]['title'] + ' per ' + str(
            number_of_queries) + ' executions vs Number of threads')
        plt.xlabel('Number of Threads')
        plt.ylabel(metric_labels[metric]['y_label'] + ' - log scale')
        plt.yscale('log')
        # Place the legend outside the plot
        plt.legend(loc='upper left', bbox_to_anchor=(1, 1))
        plt.grid(False)
        plt.xticks(sorted(threads))  # Ensure all thread counts are shown
        plt.tight_layout()
        plt.savefig(f'changing_threads/total_line_{metric}_threads.png')
        #plt.show()


def main():
    # Directory where Round folders are located
    base_directory = '/home/michalis/Documents/UofT/MSRG/Carbon/Artemis/Experiments/tpch/cpu/'
    directory_pattern = re.compile(r'cpu_(\d{1,2})_sf_\d+gb')
    for exp_folder in os.listdir(base_directory):
        match = directory_pattern.search(exp_folder)
        num_threads = 0
        if match:
            num_threads = int(match.group(1))
        round_path = os.path.join(base_directory, exp_folder)
        for round_folder in os.listdir(round_path):
            if round_folder.startswith("Round"):
                round_directory = os.path.join(round_path, round_folder)
                if os.path.isdir(round_directory):
                    aggregate_metrics(round_directory, num_threads)

    calculate_carbon()
    calculate_water()

    print(average_data)
    print(total_data)

    plot_lines(average_data)



if __name__ == "__main__":
    main()

import glob
import os
import re

import matplotlib
import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
from matplotlib.ticker import ScalarFormatter, FuncFormatter

matplotlib.use('Qt5Agg')
import seaborn as sns

# List of databases
databases = ['DuckDB', 'Hyper', 'Starrocks', 'MonetDB']

average_data = {}  # Holds the average of all experiments of the average values of different metrics

metric_labels = {
    'dram_avgE(J/sec)': {
        'title': 'DRAM average power per query',
        'y_label': 'Watt(J/sec)'
    },
    'cpu_avgE(J/sec)': {
        'title': 'CPU average power per query',
        'y_label': 'Watt(J/sec)'
    },
    'total_avgE(J/sec)': {
        'title': 'Total average power per query',
        'y_label': 'Watt(J/sec)'
    }
}


def convert_to_gb(size_string):
    # Remove 'GB' and convert to integer
    return int(size_string.replace('GB', ''))

def extract_all_columns(df):
    return df.columns.tolist()

def aggregate_metrics(directory, dram_size):
    columns_to_process = ['dram_avgE(J/sec)', 'cpu_avgE(J/sec)']

    for db in databases:
        # Initialize dictionary for the database if not already present
        if db not in average_data:
            average_data[db] = {}
        if dram_size not in average_data[db]:
            average_data[db][dram_size] = {}
            average_data[db][dram_size]['total_avgE(J/sec)'] = 0
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

            # Average values of selected metrics for all the queries' benchmark
            average_queries_values = df_filtered[columns_to_process].mean()

            for metric, avg_value in average_queries_values.items():
                if metric not in average_data[db][dram_size]:
                    average_data[db][dram_size][metric] = 0
                    average_data[db][dram_size][metric] = average_data[db][dram_size][metric] + avg_value
                average_data[db][dram_size][metric] = (average_data[db][dram_size][
                                                           metric] + avg_value) / 2  # Running average of average metric for all experiments
                average_data[db][dram_size]['total_avgE(J/sec)'] = (average_data[db][dram_size]['total_avgE(J/sec)'] + average_data[db][dram_size][metric])/2


def format_func(value, tick_number):
    return f'{value:,.2f}'  # Two decimal places and thousand separators

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
    # Melt the DataFrame to long format
    df_melted = avg_df.melt(id_vars=['Database', 'DRAM Size'], var_name='Metric', value_name='Value')
    df_log = avg_df.applymap(lambda x: np.log10(x) if isinstance(x, (int, float)) and x > 0 else x)

    # List of metrics to plot
    metrics_to_plot = ['dram_avgE(J/sec)', 'cpu_avgE(J/sec)','total_avgE(J/sec)']
    # Plotting each metric
    for metric in metrics_to_plot:
        plt.figure(figsize=(10, 6))
        sns.barplot(x='Database', y=metric, hue='DRAM Size', data=avg_df, errorbar=None)

        #plt.yscale('log')
        plt.title(metric_labels[metric]['title'],fontsize=18,pad=15 )
        plt.ylabel(metric_labels[metric]['y_label'],fontsize=14 )
        plt.tick_params(axis='y', labelsize=14)
        plt.xlabel('Databases',fontsize=18,labelpad=15)
        plt.legend(title='DRAM Size', title_fontsize=16, bbox_to_anchor=(1, 1),fontsize=14,loc='best')
        plt.xticks(rotation=0,fontsize=16)
        plt.tight_layout()
        plt.savefig(
            f'/home/michalis/Documents/UofT/MSRG/Carbon/Artemis/Supplementary_experiments/tpch/dram/plotting/changing_dram/average_bar_{metric.replace("J/sec", "W").replace("E", "P")}_comparison.png')
        #plt.show()
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

    plot_grouped_bars()


if __name__ == "__main__":
    main()
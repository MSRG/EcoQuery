import glob
import os
import re

import matplotlib
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# List of databases
databases = ['DuckDB', 'Hyper', 'Starrocks', 'MonetDB']

# Initialize the dictionary to store data
data = {}

averages = {}
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
    'cpu_dram_carbon(gCO₂eq)':{
        'title': 'CPU+DRAM carbon emissions',
        'y_label': 'gCO₂eq'
    },
    'cpu_dram_water(L)': {
        'title': 'CPU+DRAM water consumption',
        'y_label': 'Liters'
    }
}
metrics_to_plot = ['dram_totalE(J)', 'cpu_totalE(J)', 'dram_carbon(gCO₂eq)', 'cpu_carbon(gCO₂eq)']

ontario_carbon_intensity = 85
ontario_energy_mix = {
    "Biomass": 0.09 / 100,
    "Wind": 6.85 / 100,
    "Nuclear": 58.96 / 100,
    "Natural gas": 15.48 / 100,
    "Hydropower": 18.62 / 100
}
number_of_queries = 1000

# Liters/Joule
#water_consumption = {
 #   "Biomass": 0.000156,
 #   "Hydropower": 1.51e-5,
 #   "Nuclear": 6.78e-7,
 #   "Oil": 4.95e-7,
  #  "Coal and lignite": 4.94e-7,
  #  "Geothermal": 3.4e-7,
  #  "Natural gas": 2.46e-7,
 #   "Solar": 5e-8,
 #   "Wind": 2e-10
#}

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


# Function to extract all unique columns from a dataframe
def extract_all_columns(df):
    return df.columns.tolist()


def aggregate_metrics(directory):
    # Loop through each database
    for db in databases:
        # Initialize dictionary for the database if not already present
        if db not in data:
            data[db] = {}
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
            # Filter out the rows that are relevant to TPC-H queries
            #df_filtered = df[df['Label'].str.contains('tpch')]
            df_filtered = df.iloc[:-2]  # Exclude the last two rows

            # Get all unique columns dynamically
            all_columns = extract_all_columns(df_filtered)

            # Loop through each row in the filtered dataframe
            for index, row in df_filtered.iterrows():
                query = row['Label']

                # Initialize dictionary for the query if not already present
                if query not in data[db]:
                    data[db][query] = {}

                # Loop through each column
                for column in all_columns[1:]:
                    if column not in data[db][query]:
                        data[db][query][column] = []

                    # Append the column value to the respective list
                    data[db][query][column].append(row[column])


def main():
    # Directory where Round folders are located
    base_directory = '/home/michalis/Documents/UofT/MSRG/Carbon/Artemis/Supplementary_experiments/tpch/dram/dram_512gb_sf_300gb'

    # Traverse each Round directory
    for round_folder in os.listdir(base_directory):
        if round_folder.startswith("Round"):
            round_directory = os.path.join(base_directory, round_folder)
            if os.path.isdir(round_directory):
                aggregate_metrics(round_directory)

    # Calculate average values
    calculate_averages()
    print(averages)

    plot_bubble_plots()

    # Plot bar charts for each database
    plot_bars()


# print(data['Hyper'])


# Function to calculate averages for each database
def calculate_averages():
    for db in databases:
        if db not in averages:
            averages[db] = {}

    energy_columns = ['dram_avgE(J/sec)', 'dram_totalE(J)', 'cpu_avgE(J/sec)', 'cpu_totalE(J)']
    carbon_columns = ['dram_carbon(gCO₂eq)', 'cpu_carbon(gCO₂eq)']

    for database, queries in data.items():
        for query, metrics in queries.items():
            if 'idle state' == query:
                continue
            if query not in averages[database]:
                averages[database][query] = {}
            for column in energy_columns:
                avg_value = sum(data[database][query][column]) / len(data[database][query][column])
                # Scale by number of queries, except for 'load data'
                if query != 'load data':
                    avg_value *= number_of_queries
                averages[database][query][column] = avg_value
            averages[database][query]['dram_carbon(gCO₂eq)'] = (averages[database][query]['dram_totalE(J)'] / (
                    3.6 * 10 ** 6)) * ontario_carbon_intensity
            averages[database][query]['cpu_carbon(gCO₂eq)'] = (averages[database][query]['cpu_totalE(J)'] / (
                    3.6 * 10 ** 6)) * ontario_carbon_intensity
            averages[database][query]['cpu_dram_carbon(gCO₂eq)'] = averages[database][query]['cpu_carbon(gCO₂eq)'] + averages[database][query]['dram_carbon(gCO₂eq)']

            # Water consumption calculation
            for energy_source, water_footprint in ontario_energy_mix.items():
                averages[database][query]['dram_water(L)'] = averages[database][query][
                                                                 'dram_totalE(J)'] * water_footprint * \
                                                             water_consumption_intensity[energy_source] * number_of_queries
                averages[database][query]['cpu_water(L)'] = averages[database][query][
                                                                'cpu_totalE(J)'] * water_footprint * water_consumption_intensity[
                                                                energy_source] * number_of_queries
                averages[database][query]['cpu_dram_water(L)'] = averages[database][query]['cpu_water(L)'] + averages[database][query]['dram_water(L)']


def plot_bubble_plots():
    # Define a color map for the databases
    colors = {
        'DuckDB': 'blue',
        'Hyper': 'green',
        'MonetDB': 'red',
        'Starrocks': 'purple'
    }

    # Specify metrics to plot
    metrics_to_plot = ['dram_totalE(J)', 'cpu_totalE(J)', 'dram_carbon(gCO₂eq)', 'cpu_carbon(gCO₂eq)', 'dram_water(L)',
                       'cpu_water(L)','cpu_dram_carbon(gCO₂eq)','cpu_dram_water(L)']

    for metric in metrics_to_plot:
        fig, ax = plt.subplots(figsize=(10, 8))
        queries = list(averages[databases[0]].keys())
        queries = [query for query in queries if query != 'idle state' and query != 'load data']
        x_positions = np.arange(len(queries))

        for i, db in enumerate(databases):
            for j, query in enumerate(queries):
                # Calculate y position based on values
                y_position = averages[db][query][metric]
                # Calculate bubble size (e.g., based on a scaling factor or directly from data)
                bubble_size = 100

                ax.scatter(x_positions[j], y_position, s=bubble_size, alpha=0.5, label=db if j == 0 else "",
                           color=colors[db])

        # Add vertical lines for each query
        for pos in x_positions:
            ax.axvline(x=pos, color='gray', linestyle='--', linewidth=0.5)

        # Set x-axis labels and ticks
        ax.set_xticks(x_positions)
        labels = clean_labels(queries)
        ax.set_xticklabels(labels, rotation=45, ha='center', fontsize=14)
        ax.set_xlabel('Queries', labelpad=10, fontsize=18, fontweight='bold')  # Increased from 16 to 18 and made bold

        # Set y-axis label
        ax.set_ylabel(metric_labels[metric]['y_label'] + ' - log scale', fontsize=14)

        # Title and legend
        ax.set_title(metric_labels[metric]['title'] + ' per '+str(number_of_queries)+ ' executions',fontsize=18)
        ax.legend(title='Databases', title_fontsize=16, loc='upper left', bbox_to_anchor=(1, 1),fontsize=14)
        ax.set_yscale('log')  # Set y-axis to log scale
        ax.tick_params(axis='y', labelsize=14)

        plt.tight_layout()
        plt.savefig(f'/home/michalis/Documents/UofT/MSRG/Carbon/Artemis/Supplementary_experiments/tpch/dram/plotting/full_dram/bubble_{metric}_comparison.png', dpi =500, bbox_inches='tight')

        # Plot for 'load data'
        for metric in metrics_to_plot:
            fig, ax = plt.subplots(figsize=(10, 6))
            x_position = 5

            for i, db in enumerate(databases):
                # Calculate y position based on values
                y_position = averages[db]['load data'][metric]
                # Calculate bubble size (e.g., based on a scaling factor or directly from data)
                bubble_size = 100

                ax.scatter(x_position, y_position, s=bubble_size, alpha=0.5, label=db,
                           color=colors[db])

            # Add vertical lines for each query
            ax.axvline(x=x_position, color='gray', linestyle='--', linewidth=0.5)

            # Set x-axis labels and ticks
            ax.set_xticks([x_position])
            ax.set_xticklabels(['Load data'])
            #ax.set_xlabel('Databases')

            # Set y-axis label
            ax.set_ylabel(metric_labels[metric]['y_label'] + ' - log scale')

            # Title and legend
            ax.set_title(metric_labels[metric]['title'] + ' per '+str(number_of_queries)+ ' executions')
            ax.legend(title='Databases', loc='upper left', bbox_to_anchor=(1, 1))
            ax.set_yscale('log')  # Set y-axis to log scale

            plt.tight_layout()
            plt.savefig(f'/home/michalis/Documents/UofT/MSRG/Carbon/Artemis/Supplementary_experiments/tpch/dram/plotting/full_dram/bubble_{metric}_load_data_comparison.png',dpi=500)
            plt.close()
    plt.close('all')


# Function to plot the bars
def plot_bars():
    # Specify metrics to plot
    metrics_to_plot = ['dram_totalE(J)', 'cpu_totalE(J)', 'dram_carbon(gCO₂eq)', 'cpu_carbon(gCO₂eq)', 'dram_water(L)',
                       'cpu_water(L)']

    for metric in metrics_to_plot:
        fig, ax = plt.subplots(figsize=(15, 10))
        queries = list(averages[databases[0]].keys())
        queries = [query for query in queries if query != 'idle state' and query != 'load data']

        bar_width = 0.2
        index = range(len(queries))

        for i, db in enumerate(databases):
            values = [averages[db][query][metric] for query in queries]
            ax.bar([x + i * bar_width for x in index], values, bar_width, label=db)

        ax.set_xlabel('Queries')
        ax.set_ylabel(metric_labels[metric]['y_label'] + ' - log scale')
        ax.set_title(metric_labels[metric]['title'] + ' per '+str(number_of_queries)+ ' executions')
        ax.set_xticks([x + bar_width for x in index])
        ax.set_xticklabels(queries, rotation=45, ha='right')
        ax.legend()
        ax.grid(True)
        ax.set_yscale('log')  # Set y-axis to log scale

        plt.tight_layout()

        # Save the plot to a file
        plt.savefig(f'/home/michalis/Documents/UofT/MSRG/Carbon/Artemis/Supplementary_experiments/tpch/dram/plotting/full_dram/bar_{metric}_comparison.png')  # Adjust the filename as needed

        # Plot for 'load data'
        for metric in metrics_to_plot:
            fig, ax = plt.subplots(figsize=(10, 6))
            bar_width = 0.2
            index = range(len(databases))

            values = [averages[db]['load data'][metric] for db in databases]
            ax.bar([x + bar_width for x in index], values, bar_width)

            ax.set_xlabel('Databases')
            ax.set_ylabel(metric_labels[metric]['y_label'] + ' - log scale')
            ax.set_title(metric_labels[metric]['title'] + ' per '+str(number_of_queries)+ ' executions')
            ax.set_xticks([x + bar_width for x in index])
            ax.set_xticklabels(databases, rotation=45, ha='right')
            ax.grid(True)
            ax.set_yscale('log')  # Set y-axis to log scale

            plt.tight_layout()

            # Save the plot to a file
            plt.savefig(f'/home/michalis/Documents/UofT/MSRG/Carbon/Artemis/Supplementary_experiments/tpch/dram/plotting/full_dram/bar_{metric}_load_data_comparison.png')
            plt.close()
    # Once all plots are saved, you can close the figures to release resources
    plt.close('all')


def clean_labels(labels):
    cleaned_labels = []
    for label in labels:
        if label.startswith("tpch-"):
            label = label.replace("tpch-", "")
            label = label.capitalize()
        cleaned_labels.append(label)
    return cleaned_labels


if __name__ == "__main__":
    main()

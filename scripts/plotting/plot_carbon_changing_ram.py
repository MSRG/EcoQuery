import glob
import os
import re

import matplotlib
import numpy as np
import pandas as pd
from matplotlib import pyplot as plt, image as mpimg, patches, gridspec
from matplotlib.gridspec import GridSpec
from matplotlib.offsetbox import OffsetImage,AnnotationBbox
from PIL import Image

matplotlib.use('Qt5Agg')
import seaborn as sns

# List of databases
databases = ['DuckDB', 'Hyper', 'Starrocks', 'MonetDB']

average_data = {}  # Holds the average of all experiments of the average values of different metrics
total_data = {} # Holds the average  of all experiments of the total sum values of different metrics
box_plot_total_data = {} # Holds the total sum values from each experiment to create box plots for each metric

ontario_carbon_intensity = 85
ontario_energy_mix = {
    "Biomass": 0.09 / 100,
    "Wind": 6.85 / 100,
    "Nuclear": 58.96 / 100,
    "Natural gas": 15.48 / 100,
    "Hydropower": 18.62 / 100
}
scale_factor = 1000 # Scale factor

# Liters/Joule
#water_consumption_intensity = {
 #   "Biomass": 0.000156,
 #   "Hydropower": 1.51e-5,
 #   "Nuclear": 6.78e-7,
#    "Oil": 4.95e-7,
#    "Coal and lignite": 4.94e-7,
#    "Geothermal": 3.4e-7,
 #   "Natural gas": 2.46e-7,
#    "Solar": 5e-8,
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



def load_and_resize_image(path, size=32):
    """Load an image and resize it to desired size with better error handling"""
    try:
        # Get the absolute path if the path is relative
        if not os.path.isabs(path):
            script_dir = os.path.dirname(os.path.abspath(__file__))
            abs_path = os.path.join(script_dir, path)
        else:
            abs_path = path

        if not os.path.exists(abs_path):
            print(f"Warning: Image file not found at {abs_path}")
            return None

        img = Image.open(abs_path)
        img = img.convert('RGBA')
        img = img.resize((size, size), Image.Resampling.LANCZOS)  # Using LANCZOS for better quality
        return img
    except Exception as e:
        print(f"Error loading image {path}: {str(e)}")
        return None


def extract_all_columns(df):
    return df.columns.tolist()


def aggregate_metrics(directory, dram_size):
    columns_to_process = ['dram_totalE(J)', 'cpu_totalE(J)','Latency(sec)']

    for db in databases:
        # Initialize dictionary for the database if not already present
        if db not in average_data:
            average_data[db] = {}
            total_data[db] = {}
            box_plot_total_data[db] = {}
        if dram_size not in average_data[db]:
            average_data[db][dram_size] = {}
            total_data[db][dram_size] = {}
            box_plot_total_data[db][dram_size] = {}
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
                    box_plot_total_data[db][dram_size][metric] = []
                    #box_plot_total_data[db][dram_size][metric].append(sum_value)
                total_data[db][dram_size][metric] = (total_data[db][dram_size][
                                                         metric] + sum_value) / 2  # Running average of total metric for all experiments
                box_plot_total_data[db][dram_size][metric].append(sum_value)


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
            box_plot_total_data[database][dram_size]['dram_carbon(gCO₂eq)'] = []
            for dram_energy_value in box_plot_total_data[database][dram_size]['dram_totalE(J)']:
                box_plot_total_data[database][dram_size]['dram_carbon(gCO₂eq)'].append((dram_energy_value * scale_factor / (
                                                                                3.6 * 10 ** 6)) * ontario_carbon_intensity)

            box_plot_total_data[database][dram_size]['cpu_carbon(gCO₂eq)'] = []
            for cpu_energy_value in box_plot_total_data[database][dram_size]['cpu_totalE(J)']:
                box_plot_total_data[database][dram_size]['cpu_carbon(gCO₂eq)'].append(
                    (cpu_energy_value * scale_factor / (3.6 * 10 ** 6)) * ontario_carbon_intensity)





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

            box_plot_total_data[database][dram_size]['dram_water(L)'] = []
            for dram_energy_value in box_plot_total_data[database][dram_size]['dram_totalE(J)']:
                dram_water = 0
                for energy_source, water_footprint in ontario_energy_mix.items():
                    dram_water = dram_water + water_footprint * dram_energy_value * water_consumption_intensity[energy_source] * scale_factor
                box_plot_total_data[database][dram_size]['dram_water(L)'].append(dram_water)

            box_plot_total_data[database][dram_size]['cpu_water(L)'] = []
            for dram_energy_value in box_plot_total_data[database][dram_size]['cpu_totalE(J)']:
                cpu_water = 0
                for energy_source, water_footprint in ontario_energy_mix.items():
                    cpu_water = cpu_water + water_footprint * dram_energy_value * water_consumption_intensity[
                        energy_source] * scale_factor
                box_plot_total_data[database][dram_size]['cpu_water(L)'].append(cpu_water)


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



def plot_carbon_grouped_bars_reference_values(total_df, metric):
    # Define icon paths only for carbon metrics
    carbon_icon_paths = {
        'coal': 'icons/coal.jpeg',
        'gas': 'icons/gas.jpeg',
        'car': 'icons/car.png',
        'smartphone': 'icons/smartphone.png',
        'thermostat': 'icons/thermostat.png'
    }

    # Define reference values for carbon metrics
    carbon_reference_values = {
        'coal': (950, carbon_icon_paths['coal'], 350),
        'car': (4000, carbon_icon_paths['car'], 1000),
        'smartphone': (15.2, carbon_icon_paths['smartphone'], 7),
        'thermostat': (1.05, carbon_icon_paths['thermostat'], 0.5)
        # 'gas': (910,carbon_icon_paths['gas'])
    }

    carbon_labels = {
        'coal': '1 Pound of coal burned',
        'gas': '1 Gallon of gasoline consumed',
        'car': '10 Miles driven',
        'smartphone': 'Phone charge',
        'thermostat': '1 Hour of thermostat'
    }

    # Create figure
    fig = plt.figure(figsize=(14, 8))

    # Create axes with specific position to leave room for icons
    ax = plt.axes([0.1, 0.1, 0.7, 0.8])

    # Remove top and right spines
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)

    # Get unique databases and DRAM sizes
    n_databases = len(total_df['Database'].unique())
    n_drams = len(total_df['DRAM Size'].unique())
    width = 0.3  # Width of each bar

    # Calculate positions for each group
    group_spacing = 1.8  # Increase this value to add more space between groups
    start_position = 1
    indices = start_position + np.arange(n_databases) * (1 + group_spacing)  # Multiply by (1 + spacing) to add extra space

    # Create the main plot
    #ax = plt.gca()
    #sns.barplot(x='Database', y=metric, hue='DRAM Size', data=total_df, errorbar=None,width=1)

    dram_sizes = total_df['DRAM Size'].unique()

    dram_colors = ['#2E86AB', '#A23B72', '#F18F01', '#C73E1D', '#3B1F2B', '#06A77D']

    for i, dram in enumerate(dram_sizes):
        dram_data = total_df[total_df['DRAM Size'] == dram]
        offset = width * (i - (n_drams - 1) / 2)  # Center the bars
        plt.bar(indices + offset, dram_data['dram_carbon(gCO₂eq)'],
                width, label=dram, color=dram_colors[i])

    # Set the x-axis labels
    plt.xticks(indices, total_df['Database'].unique(),fontsize=14)

    # Sort reference values by value for better visualization
    sorted_references = dict(sorted(carbon_reference_values.items(), key=lambda x: x[1][0]))

    colors = ['red', 'orange', 'green', 'purple']  # Colors for each line


    for (label, (value, icon_path,offset)), color in zip(sorted_references.items(), colors):

        ax.axhline(y=value, color='#666666', linestyle='--', alpha=0.7)

        # Load and display icon
        icon = mpimg.imread(icon_path)
        # Create smaller version of the icon
        imagebox = OffsetImage(icon, zoom=0.2)  # Adjust zoom factor to change icon size

        # Position the icon at the end of the line
        ab = AnnotationBbox(imagebox, (12, value),xycoords='data',
                            frameon=False,
                            box_alignment=(0, 0.5))
        ax.add_artist(ab)

        # Add label under the icon
        plt.text(12.2, value-offset, carbon_labels[label],
                 horizontalalignment='center',
                 verticalalignment='top',fontsize=14)

        # Add colored line segment next to icon for reference
        #ax2.axhline(y=y_pos, xmin=0.25, xmax=0.35, color=color, linestyle='--', alpha=0.7)

    # Adjust plot limits to make room for icons and label
    #plt.ylim(-1.5, 1.5)  # Adjust y axis as needed
    plt.xlim(min(indices) - 1.2, 12)  # Extended to 12

    plt.yscale('log')
    ax.tick_params(axis='y', labelsize=14)
    ax.tick_params(axis='y', which='minor', length=0)
    #plt.tight_layout()
    plt.title(f'Total ' + metric_labels[metric]['title'] + ' for TPC-H for ' + str(scale_factor) + ' executions',fontsize=18)
    plt.xlabel('Databases',labelpad=15,fontsize=18, fontweight='bold')
    plt.ylabel('gCO2eq-log scale', fontsize=14)

    # Place legend outside of plot
    plt.legend(title='DRAM Size',
               title_fontsize=16,
               bbox_to_anchor=(1.13, 0.5),  # Position legend to the right of plot
               loc='center left',  # Anchor point on legend
               borderaxespad=0,
               fontsize=14)

    # Add more space on the right for icons and labels
    plt.subplots_adjust(right=0.85)

    plt.savefig(f'/home/michalis/Documents/UofT/MSRG/Carbon/Artemis/Supplementary_experiments/tpch/dram/plotting/changing_dram/total_bar_{metric}_comparison_reference_values.png')

    #plt.show()





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
    metrics_to_plot = ['dram_totalE(J)', 'cpu_totalE(J)', 'dram_carbon(gCO₂eq)', 'cpu_carbon(gCO₂eq)', 'dram_water(L)',
                       'cpu_water(L)','Latency(sec)']
    # Plotting each metric
    for metric in metrics_to_plot:
        plt.figure(figsize=(10, 6))
        sns.barplot(x='Database', y=metric, hue='DRAM Size', data=avg_df, errorbar=None)
        plt.yscale('log')
        plt.title('Average query ' + metric_labels[metric]['title'] + ' per query for ' + str(scale_factor) + ' executions')
        plt.ylabel(metric_labels[metric]['y_label'] + ' - log scale')
        plt.xlabel('Database')
        plt.legend(title='DRAM Size', bbox_to_anchor=(1, 1))
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(f'/home/michalis/Documents/UofT/MSRG/Carbon/Artemis/Supplementary_experiments/tpch/dram/plotting/changing_dram/average_bar_{metric}_comparison.png')
        #plt.show()
        plt.close()

    total_flattened_data = []
    for db, dram_sizes in total_data.items():
        for size, metrics in dram_sizes.items():
            # Extract the numeric part of DRAM size (e.g., '96gb' -> 96)
            gb_size = int(size[:-2])
            entry = {'Database': db, 'DRAM Size': size.upper(), 'GB Size': gb_size}
            # Update the entry with metric key-value pairs
            entry.update(metrics)
            # Append the entry to the list
            total_flattened_data.append(entry)

    total_df = pd.DataFrame(total_flattened_data)
    total_df = total_df.sort_values(by=['Database', 'GB Size'])
    # Melt the DataFrame to long format
    df_melted = total_df.melt(id_vars=['Database', 'DRAM Size'], var_name='Metric', value_name='Value')
    df_log = total_df.applymap(lambda x: np.log10(x) if isinstance(x, (int, float)) and x > 0 else x)

    # List of metrics to plot
    metrics_to_plot = ['dram_totalE(J)', 'cpu_totalE(J)', 'dram_carbon(gCO₂eq)', 'cpu_carbon(gCO₂eq)', 'dram_water(L)',
                       'cpu_water(L)','Latency(sec)']
    # Plotting each metric
    for metric in metrics_to_plot:
        plt.figure(figsize=(10, 6))
        sns.barplot(x='Database', y=metric, hue='DRAM Size', data=total_df, errorbar=None)
        plt.yscale('log')
        plt.title('Total ' + metric_labels[metric]['title'] + ' for TPC-H for '+ str(scale_factor) +' executions')
        plt.ylabel(metric_labels[metric]['y_label'] + ' - log scale')
        plt.xlabel('Database')
        plt.legend(title='DRAM Size', bbox_to_anchor=(1, 1))
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(f'/home/michalis/Documents/UofT/MSRG/Carbon/Artemis/Supplementary_experiments/tpch/dram/plotting/changing_dram/total_bar_{metric}_comparison.png')
        plt.close()

        if 'carbon' in metric.lower():
            plot_carbon_grouped_bars_reference_values(total_df, metric)


def plot_grouped_box_plots():
    box_plot_data = []
    for database in box_plot_total_data.keys():
        for dram_size in box_plot_total_data[database].keys():
            for metric, values in box_plot_total_data[database][dram_size].items():
                for value in values:
                    box_plot_data.append([database, dram_size, metric, value])

    # Create a DataFrame
    df = pd.DataFrame(box_plot_data, columns=['Database', 'DRAM_Size', 'Metric', 'Value'])
    dram_order = sorted(df['DRAM_Size'].unique(), key=lambda x: int(''.join(filter(str.isdigit, x))))
    df['DRAM_Size'] = pd.Categorical(df['DRAM_Size'], categories=dram_order, ordered=True)

    # Set the style of the visualization
    sns.set(style="whitegrid")

    # Get the unique metrics
    unique_metrics = df['Metric'].unique()
    # Loop over each metric and create a plot
    for metric in unique_metrics:
        plt.figure(figsize=(12, 8))

        # Filter the data for the current metric
        metric_data = df[df['Metric'] == metric]

        # Create the box plot
        ax = sns.boxplot(x='DRAM_Size', y='Value', hue='Database', data=metric_data, palette="Set3",dodge=True,width=0.6)

        # Set y-axis to log scale
        plt.ylabel(metric_labels[metric]['y_label'] + ' - log scale')
        # Set y-axis to log scale
        plt.yscale('log')

        # Add vertical dashed lines for each DRAM_Size group
        dram_sizes = df['DRAM_Size'].cat.categories
        for i, dram_size in enumerate(dram_sizes):
            ax.axvline(x=i - 0.5, color='gray', linestyle='--', linewidth=1.5)

        # Add a title
        plt.title(f'Grouped Box Plot for {metric_labels[metric]["title"]}')

        # Display the plot
        #plt.show()
        plt.savefig(f'/home/michalis/Documents/UofT/MSRG/Carbon/Artemis/Supplementary_experiments/tpch/dram/plotting/changing_dram/total_sum_box_plot_{metric}_comparison.png')


        plt.close()


def main():
    # Directory where Round folders are located
    #base_directory = '/home/michalis/Documents/UofT/MSRG/Carbon/Artemis/Experiments/tpch/dram/'
    base_directory = '/home/michalis/Documents/UofT/MSRG/Carbon/Artemis/Supplementary_experiments/tpch/dram/'
    #ignore_dram = ['96gb']
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

    print(average_data)
    print(total_data)

    df_avg_cpu_water_per_source = extract_water_sources(average_data, 'cpu_water_sources')
    df_avg_dram_water_per_source = extract_water_sources(average_data, 'dram_water_sources')

    plot_stacked_grouped_bars(df_avg_cpu_water_per_source, 'Average','cpu_water_sources')
    plot_stacked_grouped_bars(df_avg_dram_water_per_source, 'Average', 'dram_water_sources')

    df_total_cpu_water_per_source = extract_water_sources(total_data, 'cpu_water_sources')
    df_total_dram_water_per_source = extract_water_sources(total_data, 'dram_water_sources')

    plot_stacked_grouped_bars(df_total_cpu_water_per_source, 'Total', 'cpu_water_sources')
    plot_stacked_grouped_bars(df_total_dram_water_per_source, 'Total', 'dram_water_sources')

    plot_grouped_bars()

    plot_grouped_box_plots()

    plot_water_stacked_grouped_bars_reference_values(df_total_cpu_water_per_source, 'Total', 'cpu_water_sources')
    plot_water_stacked_grouped_bars_reference_values(df_total_dram_water_per_source, 'Total', 'dram_water_sources')


def plot_water_stacked_grouped_bars_reference_values(df_expanded, title_prefix, metric):
    # Define icon paths only for carbon metrics
    water_icon_paths = {
        'drinking': 'icons/drinking.png',
        #'toilet': 'icons/toilet.jpeg',
        #'washing machine': 'icons/washing_machine.png',
        'dishwasher': 'icons/dishwasher.png',
        'shower': 'icons/shower.png',
        'coffee': 'icons/coffee.png'
    }

    # Define reference values for carbon metrics
    water_reference_values = {
        'drinking': (2, water_icon_paths['drinking'], 0.8),
        #'toilet': (6, water_icon_paths['toilet'], 1),
        #'washing machine': (50, water_icon_paths['washing machine'], 1),
        'dishwasher': (15, water_icon_paths['dishwasher'], 7),
        'shower': (90,water_icon_paths['shower'],43),
        'coffee': (0.2,water_icon_paths['coffee'],0.08)
    }

    water_labels = {
        'drinking': 'Drinking Water (daily)',
        #'toilet': 'Flushing Toilet',
        #'washing machine': 'Washing Machine Load',
        'dishwasher': 'Dishwasher Load',
        'shower': 'Average Shower (10 min)',
        'coffee': 'Making a cup of coffee'
    }

    sizes = df_expanded['DRAM'].unique()

    # Set up plot
    #fig, ax = plt.subplots(figsize=(18, 10))  # Further increased figure size

    fig = plt.figure(figsize=(14, 8))
    ax = plt.axes([0.1, 0.1, 0.7, 0.8])

    # Colors for different sources
    colors = plt.get_cmap('tab10', len(df_expanded.columns) - 3)  # Exclude 'Database', 'DRAM', and 'DRAMValue'

    # Plot parameters
    bar_width = 0.3
    opacity = 0.8
    group_spacing = 0.6  # Increased spacing between groups
    bar_spacing = 0.1  # Spacing between bars within a group

    group_positions = np.arange(len(databases)) * (len(sizes) * (bar_width + bar_spacing) + group_spacing)

    start_position = 1
    #indices = start_position + np.arange(len(databases)) * (1 + group_spacing)  # Multiply by (1 + spacing) to add extra space

    for i, size in enumerate(sizes):
        size_values = df_expanded[df_expanded['DRAM'] == size].set_index('Database').drop(['DRAM', 'DRAMValue'], axis=1)
        size_values = size_values.reindex(databases)
        bottom = np.zeros(len(databases))

        bar_positions = group_positions + i * (bar_width + bar_spacing)

        # Create the stacked bars
        for j, source in enumerate(size_values.columns):
            values = size_values[source].values
            rects = ax.bar(bar_positions, values, bar_width, bottom=bottom, alpha=opacity,
                           label=source if i == 0 else "", color=colors(j))
            bottom += values

        # Label each bar with its DRAM size
        for k, pos in enumerate(bar_positions):
            ax.text(pos, -0.01, size, ha='center', va='top', fontsize=10, rotation=45,
                    transform=ax.get_xaxis_transform())

    ax.set_yscale('log')
    ax.tick_params(axis='y', labelsize=14)

    # Labeling and formatting
    ax.set_xlabel('Databases',fontsize=18, fontweight='bold')
    ax.set_ylabel(metric_labels[metric]['y_label'] + '- Log scale', fontsize=14)
    ax.set_title(title_prefix + " " + metric_labels[metric]['title'] + '(' + str(scale_factor) + ' TPC-H Runs)', fontsize=18,pad=25)

    # Set x-ticks at the center of each group
    ax.set_xticks(group_positions + (len(sizes) - 1) * (bar_width + bar_spacing) / 2)
    ax.set_xticklabels(databases,fontsize=14)

    # Remove top and right spines
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)

    ax.grid(False)

    # Adjust x-tick label positions
    ax.tick_params(axis='x', pad=40)  # Increased padding to accommodate DRAM size labels

    # Adjust x-label position
    ax.xaxis.set_label_coords(0.5, -0.14)

    handles, labels = ax.get_legend_handles_labels()

    ax.legend(handles[:len(size_values.columns)], labels[:len(size_values.columns)], title='Energy_Source',title_fontsize=16,
              bbox_to_anchor=(1.08, 0.5), loc='center left',fontsize=14)

    # Sort reference values by value for better visualization
    sorted_references = dict(sorted(water_reference_values.items(), key=lambda x: x[1][0]))

    #colors = ['red', 'orange', 'green', 'purple']  # Colors for each line

    for (label, (value, icon_path,offset)) in sorted_references.items():

        #ax.axhline(y=value, color='#666666', linestyle='--', alpha=0.7)

        ax.axhline(y=value, color='#666666', linestyle='--', alpha=0.7)

        # Load and display icon
        icon = mpimg.imread(icon_path)
        # Create smaller version of the icon
        imagebox = OffsetImage(icon, zoom=0.2)  # Adjust zoom factor to change icon size

        # Position the icon at the end of the line
        ab = AnnotationBbox(imagebox, (13, value),xycoords='data',
                            frameon=False,
                            box_alignment=(0, 0.5))
        ax.add_artist(ab)

        # Add label under the icon
        plt.text(13.2, value-offset, water_labels[label],
                 horizontalalignment='center',
                 verticalalignment='top')

    plt.xlim(min(group_positions) - 1.2, 14)
    ax.set_ylim(0.01, 1000)  # Adjust these values based on your min and max values

    plt.subplots_adjust(right=0.85)

    # Show plot
    #plt.tight_layout()
    plt.savefig(
        f'/home/michalis/Documents/UofT/MSRG/Carbon/Artemis/Supplementary_experiments/tpch/dram/plotting/changing_dram/{title_prefix.lower()}_grouped_stacked_bar_{metric}_comparison_reference_values.png',
        bbox_inches='tight',dpi =500)
    plt.close()




def plot_stacked_grouped_bars(df_expanded, title_prefix, metric):
    # Extract unique databases and sizes
    databases = df_expanded['Database'].unique()
    sizes = df_expanded['DRAM'].unique()

    # Set up plot
    fig, ax = plt.subplots(figsize=(18, 10))  # Further increased figure size

    # Colors for different sources
    colors = plt.get_cmap('tab10', len(df_expanded.columns) - 3)  # Exclude 'Database', 'DRAM', and 'DRAMValue'

    # Plot parameters
    bar_width = 0.13
    opacity = 0.8
    group_spacing = 0.3  # Increased spacing between groups
    bar_spacing = 0.05  # Spacing between bars within a group

    # Calculate positions for each group and bar
    group_positions = np.arange(len(databases)) * (len(sizes) * (bar_width + bar_spacing) + group_spacing)

    for i, size in enumerate(sizes):
        size_values = df_expanded[df_expanded['DRAM'] == size].set_index('Database').drop(['DRAM', 'DRAMValue'], axis=1)
        size_values = size_values.reindex(databases)
        bottom = np.zeros(len(databases))

        bar_positions = group_positions + i * (bar_width + bar_spacing)

        for j, source in enumerate(size_values.columns):
            values = size_values[source].values
            rects = ax.bar(bar_positions, values, bar_width, bottom=bottom, alpha=opacity,
                           label=source if i == 0 else "", color=colors(j))
            bottom += values

        # Label each bar with its DRAM size
        for k, pos in enumerate(bar_positions):
            ax.text(pos, -0.01, size, ha='center', va='top', fontsize=8, rotation=45, transform=ax.get_xaxis_transform())

    ax.set_yscale('log')

    # Labeling and formatting
    ax.set_xlabel('Databases')
    ax.set_ylabel(metric_labels[metric]['y_label'] + '- Log scale')
    ax.set_title(title_prefix + " " + metric_labels[metric]['title'] + '(' + str(scale_factor) + ' TPC-H Runs)')

    # Set x-ticks at the center of each group
    ax.set_xticks(group_positions + (len(sizes) - 1) * (bar_width + bar_spacing) / 2)
    ax.set_xticklabels(databases)

    # Adjust x-tick label positions
    ax.tick_params(axis='x', pad=40)  # Increased padding to accommodate DRAM size labels

    # Adjust x-label position
    ax.xaxis.set_label_coords(0.5, -0.2)

    handles, labels = ax.get_legend_handles_labels()
    ax.legend(handles[:len(size_values.columns)], labels[:len(size_values.columns)], title='Energy_Source',
              bbox_to_anchor=(1, 1), loc='upper left')

    # Show plot
    plt.tight_layout()
    plt.savefig(
        f'/home/michalis/Documents/UofT/MSRG/Carbon/Artemis/Supplementary_experiments/tpch/dram/plotting/changing_dram/{title_prefix.lower()}_grouped_stacked_bar_{metric}_comparison.png',
        bbox_inches='tight')
    plt.close()


def extract_water_sources(data, source):
    water_sources_data = {}

    for db, db_data in data.items():
        water_sources_data[db] = {}
        for config, config_data in db_data.items():
            water_sources_data[db][config] = config_data.get(source, None)

    flattened_data = []
    for db, sizes in water_sources_data.items():
        for size, sources in sizes.items():
            flattened_data.append({
                'Database': db,
                'DRAM': size,
                'Energy_Source': sources,
            })

    df = pd.DataFrame(flattened_data)
    # Expand 'Source' dictionary into columns
    df_expanded = pd.concat([df.drop(['Energy_Source'], axis=1), df['Energy_Source'].apply(pd.Series)], axis=1)
    # Extract numerical values from 'Size' and sort by these values
    df_expanded['DRAMValue'] = df_expanded['DRAM'].str.extract(r'(\d+)').astype(int)
    df_expanded = df_expanded.sort_values('DRAMValue') # sorted based on DRAM sizes
    return df_expanded


if __name__ == "__main__":
    main()

import colorsys
import glob
import os
import re

import numpy as np
import pandas as pd
from matplotlib import pyplot as plt

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
scale_factor = 10_000 # Scale factor

# Liters/Joule
water_consumption_intensity = {
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
    }
}

def extract_all_columns(df):
    return df.columns.tolist()

def calculate_carbon(disk_name):
    for database, dram_sizes in average_data[disk_name].items():
        for dram_size in dram_sizes.keys():
            average_data[disk_name][database][dram_size]['dram_carbon(gCO₂eq)'] = (average_data[disk_name][database][dram_size][
                                                                            'dram_totalE(J)'] * scale_factor / (
                                                                                3.6 * 10 ** 6)) * ontario_carbon_intensity
            average_data[disk_name][database][dram_size]['cpu_carbon(gCO₂eq)'] = (average_data[disk_name][database][dram_size][
                                                                           'cpu_totalE(J)'] * scale_factor / (
                                                                               3.6 * 10 ** 6)) * ontario_carbon_intensity

    for database, dram_sizes in total_data[disk_name].items():
        for dram_size in dram_sizes.keys():
            total_data[disk_name][database][dram_size]['dram_carbon(gCO₂eq)'] = (total_data[disk_name][database][dram_size][
                                                                          'dram_totalE(J)'] * scale_factor / (
                                                                              3.6 * 10 ** 6)) * ontario_carbon_intensity
            total_data[disk_name][database][dram_size]['cpu_carbon(gCO₂eq)'] = (total_data[disk_name][database][dram_size][
                                                                         'cpu_totalE(J)'] * scale_factor / (
                                                                             3.6 * 10 ** 6)) * ontario_carbon_intensity


def calculate_water(disk_name):

    for database, dram_sizes in total_data[disk_name].items():
        for dram_size in dram_sizes.keys():
            dram_water = 0
            cpu_water = 0
            dram_water_sources = {}
            cpu_water_sources = {}
            for energy_source, percentage_contribution in ontario_energy_mix.items():
                dram_water = dram_water + percentage_contribution * total_data[disk_name][database][dram_size]['dram_totalE(J)'] * water_consumption_intensity[energy_source] * scale_factor
                cpu_water = cpu_water + percentage_contribution * total_data[disk_name][database][dram_size]['cpu_totalE(J)'] * water_consumption_intensity[energy_source] * scale_factor
                dram_water_sources[energy_source] = percentage_contribution * total_data[disk_name][database][dram_size]['dram_totalE(J)'] * water_consumption_intensity[energy_source] * scale_factor
                cpu_water_sources[energy_source] = percentage_contribution * total_data[disk_name][database][dram_size]['cpu_totalE(J)'] * water_consumption_intensity[energy_source] * scale_factor

            total_data[disk_name][database][dram_size]['dram_water(L)'] = dram_water
            total_data[disk_name][database][dram_size]['cpu_water(L)'] = cpu_water
            total_data[disk_name][database][dram_size]['dram_water_sources'] = dram_water_sources
            total_data[disk_name][database][dram_size]['cpu_water_sources'] = cpu_water_sources

            box_plot_total_data[disk_name][database][dram_size]['dram_water(L)'] = []
            for dram_energy_value in box_plot_total_data[disk_name][database][dram_size]['dram_totalE(J)']:
                dram_water = 0
                for energy_source, percentage_contribution in ontario_energy_mix.items():
                    dram_water = dram_water + percentage_contribution * dram_energy_value * water_consumption_intensity[
                        energy_source] * scale_factor
                box_plot_total_data[disk_name][database][dram_size]['dram_water(L)'].append(dram_water)

            box_plot_total_data[disk_name][database][dram_size]['cpu_water(L)'] = []
            for dram_energy_value in box_plot_total_data[disk_name][database][dram_size]['cpu_totalE(J)']:
                cpu_water = 0
                for energy_source, percentage_contribution in ontario_energy_mix.items():
                    cpu_water = cpu_water + percentage_contribution * dram_energy_value * water_consumption_intensity[
                        energy_source] * scale_factor
                box_plot_total_data[disk_name][database][dram_size]['cpu_water(L)'].append(cpu_water)

    for database, dram_sizes in average_data[disk_name].items():
        for dram_size in dram_sizes.keys():
            dram_water = 0
            cpu_water = 0
            dram_water_sources = {}
            cpu_water_sources = {}
            for energy_source, percentage_contribution in ontario_energy_mix.items():
                dram_water = dram_water + percentage_contribution * average_data[disk_name][database][dram_size]['dram_totalE(J)'] * \
                             water_consumption_intensity[energy_source] * scale_factor
                cpu_water = cpu_water + percentage_contribution * average_data[disk_name][database][dram_size]['cpu_totalE(J)'] * \
                            water_consumption_intensity[energy_source] * scale_factor
                dram_water_sources[energy_source] = percentage_contribution * average_data[disk_name][database][dram_size][
                    'dram_totalE(J)'] * \
                                                    water_consumption_intensity[energy_source] * scale_factor
                cpu_water_sources[energy_source] = percentage_contribution * average_data[disk_name][database][dram_size][
                    'cpu_totalE(J)'] * \
                                                   water_consumption_intensity[energy_source] * scale_factor
            average_data[disk_name][database][dram_size]['dram_water(L)'] = dram_water
            average_data[disk_name][database][dram_size]['cpu_water(L)'] = cpu_water
            average_data[disk_name][database][dram_size]['dram_water_sources'] = dram_water_sources
            average_data[disk_name][database][dram_size]['cpu_water_sources'] = cpu_water_sources




def aggregate_metrics(directory, dram_size,disk_name):
    columns_to_process = ['dram_totalE(J)', 'cpu_totalE(J)']

    for db in databases:
        # Initialize dictionary for the database if not already present
        if db not in average_data[disk_name]:
            average_data[disk_name][db] = {}
            total_data[disk_name][db] = {}
            box_plot_total_data[disk_name][db] = {}
        if dram_size not in average_data[disk_name][db]:
            average_data[disk_name][db][dram_size] = {}
            total_data[disk_name][db][dram_size] = {}
            box_plot_total_data[disk_name][db][dram_size] = {}
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
            df_filtered = df.iloc[:-2]  # Exclude the last two rows
            df_load = df_filtered.iloc[1]
            df_filtered = df_filtered.iloc[2:]  # Exclude the first two rows
            # Get all unique columns dynamically
            all_columns = extract_all_columns(df_filtered)
            # Average values of selected metrics for all the queries' benchmark
            average_queries_values = df_filtered[columns_to_process].mean()

            for metric, avg_value in average_queries_values.items():
                if metric not in average_data[disk_name][db][dram_size]:
                    average_data[disk_name][db][dram_size][metric] = 0
                    average_data[disk_name][db][dram_size][metric] = average_data[disk_name][db][dram_size][metric] + avg_value
                average_data[disk_name][db][dram_size][metric] = (average_data[disk_name][db][dram_size][
                                                           metric] + avg_value) / 2  # Running average of average metric for all experiments

                # Total values of selected metrics for all the queries' benchmark
                sum_queries_values = df_filtered[columns_to_process].sum()

                for metric, sum_value in sum_queries_values.items():
                    if metric not in total_data[disk_name][db][dram_size]:
                        total_data[disk_name][db][dram_size][metric] = 0
                        total_data[disk_name][db][dram_size][metric] = total_data[disk_name][db][dram_size][metric] + sum_value
                        box_plot_total_data[disk_name][db][dram_size][metric] = []
                    total_data[disk_name][db][dram_size][metric] = (total_data[disk_name][db][dram_size][
                                                             metric] + sum_value) / 2  # Running average of total metric for all experiments
                    box_plot_total_data[disk_name][db][dram_size][metric].append(sum_value)


def create_carbon_emissions_bar_plot(data,plot_type):
    for component in ['DRAM', 'CPU']:
        storage_types = list(data.keys())
        configurations = list(data[storage_types[0]][databases[0]].keys())

        for config in configurations:
            fig, ax = plt.subplots(figsize=(12, 6))
            x = np.arange(len(databases))
            width = 0.35
            for i, storage in enumerate(storage_types):
                emissions = [data[storage][db][config][f'{component.lower()}_carbon(gCO₂eq)'] for db in databases]
                offset = width * (i - 0.5)
                bars = ax.bar(x + offset, emissions, width, label=storage.upper())

                # Add value labels on the bars
                for bar in bars:
                    height = bar.get_height()
                    ax.text(bar.get_x() + bar.get_width() / 2., height,
                            f'{int(height)}',
                            ha='center', va='bottom', rotation=45, fontsize=8,fontweight='bold',
                            transform_rotates_text=True)

            ax.set_yscale('log')
            #ax.set_ylim(bottom=0.01)
            ax.set_xlabel('Databases', fontsize=18)
            ax.set_ylabel('Carbon Emissions (gCO₂eq) - Log scale', fontsize=14)
            plt.tick_params(axis='y', labelsize=14)
            if plot_type == 'average':
                ax.set_title(f'{component} Carbon Footprint: {config} DRAM\nAverage TPC-H Query × 10\u2074 Executions',
                             fontsize=16, fontweight='bold')
            elif plot_type == 'total':
                ax.set_title(f'{component} Carbon Footprint: {config} DRAM\nTotal TPC-H benchmark × 10\u2074 Executions',
                             fontsize=16, fontweight='bold')
            #ax.set_title(f'Average {component} Carbon Emissions per query for a million executions - {config} Configuration', fontsize=14)
            ax.set_xticks(x)
            ax.set_xticklabels(databases, rotation=0, ha='center',fontsize=16)
            ax.legend(title='Storage Medium', title_fontsize=16, bbox_to_anchor=(1, 1), fontsize=14, loc='best')
            #ax.legend()
            #ax.grid(axis='y', linestyle='--', alpha=0.7)

            plt.tight_layout()
            filename = f'{plot_type}_{component.lower()}_{config.lower()}_carbon_emissions.png'
            plt.savefig(f'carbon_ssd_hdd/{filename}', dpi=300, bbox_inches='tight')
            plt.close()
            print(f"Plot saved as {filename}")

def extract_water_sources(data, source,disk_name):
    water_sources_data = {}

    for db, db_data in data[disk_name].items():
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
    df_expanded = df_expanded.sort_values('DRAMValue')  # sorted based on DRAM sizes
    return df_expanded

def generate_pastel_colors(n):
    hueStep = 1.0 / n
    pastel_colors = []
    for i in range(n):
        h = i * hueStep
        s = 0.4  # Saturation
        v = 0.95  # Value
        rgb = colorsys.hsv_to_rgb(h, s, v)
        pastel_colors.append(rgb)
    return pastel_colors

def generate_muted_colors(n):
    hueStep = 1.0 / n
    muted_colors = []
    for i in range(n):
        h = i * hueStep
        s = 0.3  # Lower saturation for more muted colors
        v = 0.7  # Lower value for darker, more muted colors
        rgb = colorsys.hsv_to_rgb(h, s, v)
        muted_colors.append(rgb)
    return muted_colors

def generate_vibrant_colors(n):
    hueStep = 1.0 / n
    vibrant_colors = []
    for i in range(n):
        h = i * hueStep
        s = 0.9  # Increased saturation for more vibrant colors
        v = 0.9   # Increased value for brighter colors
        rgb = colorsys.hsv_to_rgb(h, s, v)
        vibrant_colors.append(rgb)
    return vibrant_colors


def plot_water_usage_comparison_stacked_bar(data,plot_type):
    # Plot parameters
    bar_width = 0.13
    opacity = 0.9
    group_spacing = 0.4  # Increased spacing between groups
    bar_spacing = 0.07  # Spacing between bars within a group


    for component in ['DRAM', 'CPU']:
        storage_types = list(data.keys())
        configurations = list(data[storage_types[0]][databases[0]].keys())

        #colors = plt.get_cmap('tab10', len(databases) * len(storage_types))
        water_sources = list(data[storage_types[0]][databases[0]][configurations[0]][f'{component.lower()}_water_sources'].keys())
        colors = plt.cm.get_cmap('tab10')(np.linspace(0, 1, len(water_sources)))
        #colors = generate_muted_colors(len(water_sources))
        for config in configurations:
            fig, ax = plt.subplots(figsize=(15, 8))
            # Calculate positions for each group and bar
            group_width = len(storage_types) * (bar_width + bar_spacing) - bar_spacing
            group_positions = np.arange(len(databases)) * (len(storage_types) * bar_width + group_spacing)
            #x = np.arange(len(databases))
            #width = 0.35

            for i,db in enumerate(databases):
                for j, storage in enumerate(storage_types):
                    bottom = 1e-10  # Small non-zero value for log scale
                    bar_position = group_positions[i] + j * (bar_width + bar_spacing)
                    for k, source in enumerate(water_sources):
                        value = data[storage][db][config][f'{component.lower()}_water_sources'][source]
                        rect = ax.bar(bar_position, value, bar_width, bottom=bottom, alpha=opacity,
                                      label=f'{source}' if i == 0 and j == 0 else "",
                                      color=colors[k])
                        bottom += value


                    # Add total water usage labels on top of each bar
                    total_value = data[storage][db][config][f'{component.lower()}_water(L)']
                    ax.text(bar_position, total_value, f'{total_value:.2f}',ha='center', va='bottom', fontsize=9, rotation=45,fontweight='bold')
                    # Add storage type label under each bar
                    ax.text(bar_position, -0.02, storage.upper(),
                            ha='center', va='top', fontsize=10, transform=ax.get_xaxis_transform())

            ax.set_ylabel('Water Usage (L) - Log scale')
            ax.set_title(f'{component} Water Usage Comparison - {config} ({plot_type})\n'
                         f'Scaled for {scale_factor} TPC-H Executions', fontsize=14)
            ax.set_xticks(group_positions + group_width / 2)
            ax.set_xticklabels(databases, rotation=45, ha='right',fontweight='bold')
            # Adjust x-tick label positions to be slightly lower
            ax.tick_params(axis='x', which='major', pad=15)
            ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
            ax.set_yscale('log')
            # Set background color to light gray for better contrast with muted colors
            #ax.set_facecolor('#f0f0f0')


            plt.tight_layout()
            filename = f'{component.lower()}_water_usage_{config}_{plot_type}_comparison_stacked_plot.png'
            plt.savefig(f'carbon_ssd_hdd/{filename}', bbox_inches='tight')
            plt.close()
            print(f"Plot saved as {filename}")



def create_water_consumption_bar_plot(data,plot_type):
    for component in ['DRAM', 'CPU']:
        storage_types = list(data.keys())
        configurations = list(data[storage_types[0]][databases[0]].keys())
        for config in configurations:
            fig, ax = plt.subplots(figsize=(12, 6))
            x = np.arange(len(databases))
            width = 0.35
            for i, storage in enumerate(storage_types):
                water_consumption = [data[storage][db][config][f'{component.lower()}_water(L)'] for db in databases]
                water_consumption_distribution = [data[storage][db][config][f'{component.lower()}_water_sources'] for db in databases]

                offset = width * (i - 0.5)
                bars = ax.bar(x + offset, water_consumption, width, label=storage.upper())

                # Add value labels on the bars
                for bar in bars:
                    height = bar.get_height()
                    ax.text(bar.get_x() + bar.get_width() / 2., height,
                            f'{int(height)}',
                            ha='center', va='bottom', rotation=45, fontsize=8, fontweight='bold',
                            transform_rotates_text=True)

            ax.set_yscale('log')
            # ax.set_ylim(bottom=0.01)
            ax.set_xlabel('Databases', fontsize=18)
            ax.set_ylabel('Water consumption (L) - Log scale', fontsize=14)
            plt.tick_params(axis='y', labelsize=14)
            if plot_type == 'average':
                ax.set_title(f'{component} Water consumption: {config} DRAM\nAverage TPC-H Query × 10\u2074 Executions',
                             fontsize=16, fontweight='bold')
            elif plot_type == 'total':
                ax.set_title(
                    f'{component} Water consumption: {config} DRAM\nTotal TPC-H benchmark × 10\u2074 Executions',
                    fontsize=16, fontweight='bold')
            # ax.set_title(f'Average {component} Carbon Emissions per query for a million executions - {config} Configuration', fontsize=14)
            ax.set_xticks(x)
            ax.set_xticklabels(databases, rotation=0, ha='center',fontsize=16)
            ax.legend(title='Storage Medium', title_fontsize=16, bbox_to_anchor=(1, 1), fontsize=14, loc='best')

            plt.tight_layout()
            filename = f'{plot_type}_{component.lower()}_{config.lower()}_water_consumption.png'
            plt.savefig(f'carbon_ssd_hdd/{filename}', dpi=300, bbox_inches='tight')
            plt.close()
            print(f"Plot saved as {filename}")





def main():
    # Directory where Round folders are located
    base_directory = '/home/michalis/Documents/UofT/MSRG/Carbon/Artemis/Supplementary_experiments/tpch/ssd_hdd/'
    for disk_name in os.listdir(base_directory):
        disk_path = os.path.join(base_directory, disk_name)

        if disk_name not in average_data:
            average_data[disk_name] = {}
            total_data[disk_name] = {}
            box_plot_total_data[disk_name] = {}
        for exp_folder in os.listdir(disk_path):
            directory_pattern = re.compile(r'dram_(\d+gb)_sf_\d+gb')
            match = directory_pattern.search(exp_folder)
            dram_size = ''
            if match:
                dram_size = match.group(1)
            round_path = os.path.join(disk_path, exp_folder)
            for round_folder in os.listdir(round_path):
                if round_folder.startswith("Round"):
                    round_directory = os.path.join(round_path, round_folder)
                    if os.path.isdir(round_directory):
                        aggregate_metrics(round_directory, dram_size.upper(),disk_name)

        calculate_carbon(disk_name)
        calculate_water(disk_name)

    create_carbon_emissions_bar_plot(average_data,'average')
    create_carbon_emissions_bar_plot(total_data, 'total')
    create_water_consumption_bar_plot(average_data,'average')
    create_water_consumption_bar_plot(total_data, 'total')
    plot_water_usage_comparison_stacked_bar(total_data, 'total')





if __name__ == "__main__":
    main()
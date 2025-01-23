import ast
import glob
import math
import os
import re

import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
import matplotlib
matplotlib.use('Qt5Agg')

# List of databases
databases = ['DuckDB', 'Hyper', 'MonetDB','Starrocks' ]
columns_to_process = ['Read Count', 'Write Count','Read Bytes','Write Bytes']
disks_to_process = ['sda']

total_data = {}


def sum_list_position(dataframe, column_names, position):
    def extract_value(x, pos):
        try:
            value =  eval(x)[pos]
            return float(value)
        except(IndexError, TypeError, SyntaxError):
            return float(x) if pos == 0 else 0

    result = {}
    for column in column_names:
        if position == -1:
            result[column] = 0
        else:
            sum_value = dataframe[column].apply(lambda x: extract_value(x, position)).sum()
            result[column] = sum_value

    return result


def aggregate_metrics(directory, dram_size):

        # Initialize dictionary for the database if not already present
    for disk in disks_to_process:
        if disk not in total_data:
            total_data[disk] = {}
        for db in databases:
            if db not in total_data[disk]:
                total_data[disk][db] = {}
            if dram_size not in total_data[disk][db]:
                total_data[disk][db][dram_size] = {}
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
                active_disks = df_filtered['Device']
                disk_index = -1
                for query,active_disks_in_query in enumerate(active_disks):
                    # Convert the string to an actual list
                    try:
                        actual_disk_list = ast.literal_eval(active_disks_in_query)  # Safer than eval
                        if isinstance(actual_disk_list, list):
                            # Iterate over the list
                            for ad,active_disk in enumerate(actual_disk_list):
                                if active_disk == disk:
                                    disk_index = ad
                    except (ValueError, SyntaxError):
                        print("There are no active disks for query " + str(query))


                sum_queries_values = sum_list_position(df_filtered, columns_to_process, disk_index)
                total_latency_sum = df_filtered['Latency(sec)'].sum()
                sum_queries_values['Latency(sec)'] = total_latency_sum

                # Total values of selected metrics for all the queries' benchmark
                #sum_queries_values = df_filtered[columns_to_process].sum()

                for metric, sum_value in sum_queries_values.items():
                    if metric not in total_data[disk][db][dram_size]:
                        total_data[disk][db][dram_size][metric] = 0
                        if metric == 'Latency(sec)':
                            total_data[disk][db][dram_size][metric] = total_data[disk][db][dram_size][metric] + sum_value
                        else:
                            total_data[disk][db][dram_size][metric] = total_data[disk][db][dram_size][
                                                                          metric] + sum_value
                        continue
                    if metric =='Latency(sec)':
                        total_data[disk][db][dram_size][metric] = (total_data[disk][db][dram_size][
                                                                           metric] + sum_value) / 2.00000
                    else:
                        total_data[disk][db][dram_size][metric] = int((total_data[disk][db][dram_size][
                                                             metric] + sum_value) / 2)  # Running average of total metric for all experiments



def convert_to_gb(size_string):
    # Remove 'GB' and convert to integer
    return int(size_string.replace('GB', ''))



def plot_IO_count_comparison_stacked_bar(data):
    # Plot parameters
    bar_width = 0.13
    opacity = 0.9
    group_spacing = 0.7  # Increased spacing between groups
    bar_spacing = 0.07  # Spacing between bars within a group

    for disk in disks_to_process:
        fig, ax = plt.subplots(figsize=(15, 8))
        dram_sizes = list(data[disk][databases[0]].keys())
        dram_sorted_sizes = sorted(dram_sizes, key=convert_to_gb)
        #IO_types = list(data[disk][databases[0]][dram_sizes[0]].keys())
        IO_types = ['Read Count', 'Write Count']
        colors = plt.cm.get_cmap('tab10')(np.linspace(0, 1, len(IO_types)))

        # Calculate positions for each group and bar
        group_width = len(dram_sizes) * (bar_width + bar_spacing) - bar_spacing
        group_positions = np.arange(len(databases)) * (len(dram_sizes) * bar_width + group_spacing)

        for i, db in enumerate(databases):
            for j, dram_size in enumerate(dram_sorted_sizes):
                bottom = 1e-10  # Small non-zero value for log scale
                bar_position = group_positions[i] + j * (bar_width + bar_spacing)
                total_IO = 0
                for k, IO_type in enumerate(IO_types):
                    value = data[disk][db][dram_size][IO_type]
                    rect = ax.bar(bar_position, value, bar_width, bottom=bottom, alpha=opacity,
                                  label=f'{IO_type}' if i == 0 and j == 0 else "",
                                  color=colors[k])
                    bottom += value
                    total_IO += value

                ax.text(bar_position, total_IO, f'{int(total_IO)}', ha='center', va='bottom', fontsize=9,
                        rotation=45, fontweight='bold')
                ax.text(bar_position, -0.02, dram_size.upper(),
                        ha='center', va='top', fontsize=10, transform=ax.get_xaxis_transform(),rotation=45)

        ax.set_ylabel('I/O operations')
        #ax.set_yscale('log')
        ax.set_title(f' Total I/0 operations ', fontsize=14)
        ax.set_xticks(group_positions + group_width / 2)
        ax.set_xticklabels(databases, rotation=0, ha='right', fontweight='bold')
        # Adjust x-tick label positions to be slightly lower
        ax.tick_params(axis='x', which='major', pad=45)
        ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.tight_layout()
        filename = f'io_operations_comparison_stacked_plot_{disk}.png'
        #plt.savefig(f'/home/michalis/Documents/UofT/MSRG/Carbon/Artemis/Supplementary_experiments/tpch/dram/plotting/changing_dram/{filename}', bbox_inches='tight')
        plt.savefig(
            f'/home/michalis/Documents/UofT/MSRG/Carbon/Artemis/Supplementary_experiments/tpch/dram/correct_disk_exp/plotting/{filename}',
            bbox_inches='tight')
        plt.close()
        print(f"Plot saved as {filename}")

        # Plot parameters
        bar_width = 0.13
        opacity = 0.9
        group_spacing = 0.7
        bar_spacing = 0.07

        for disk in disks_to_process:
            fig, ax = plt.subplots(figsize=(20, 10))  # Increased width, reduced height
            dram_sizes = list(data[disk][databases[0]].keys())
            dram_sorted_sizes = sorted(dram_sizes, key=convert_to_gb)
            IO_types = ['Read Count', 'Write Count']
            colors = plt.cm.get_cmap('tab10')(np.linspace(0, 1, len(IO_types)))

            group_width = len(dram_sizes) * (bar_width + bar_spacing) - bar_spacing
            group_positions = np.arange(len(databases)) * (len(dram_sizes) * bar_width + group_spacing)

            for i, db in enumerate(databases):
                for j, dram_size in enumerate(dram_sorted_sizes):
                    bottom = 1
                    bar_position = group_positions[i] + j * (bar_width + bar_spacing)
                    total_IO = 0
                    for k, IO_type in enumerate(IO_types):
                        value = max(data[disk][db][dram_size][IO_type], 1)  # Ensure non-zero values
                        rect = ax.bar(bar_position, value, bar_width, bottom=bottom, alpha=opacity,
                                      label=f'{IO_type}' if i == 0 and j == 0 else "",
                                      color=colors[k])
                        bottom += value
                        total_IO += value

                    ax.text(bar_position, total_IO, f'{int(total_IO)}', ha='center', va='bottom', fontsize=7,
                            rotation=45, fontweight='bold')
                    ax.text(bar_position, -0.02, dram_size.upper(),
                            ha='center', va='top', fontsize=8, transform=ax.get_xaxis_transform(), rotation=45)

            ax.set_ylabel('I/O operations (log scale)')
            ax.set_yscale('log')
            ax.set_ylim(1, ax.get_ylim()[1])  # Set lower limit to 1
            ax.set_title(f'Total I/O operations', fontsize=14)
            ax.set_xticks(group_positions + group_width / 2)
            ax.set_xticklabels(databases, rotation=0, ha='right', fontweight='bold', fontsize=10)
            ax.tick_params(axis='x', which='major', pad=50)
            ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=8)

            plt.tight_layout()
            filename = f'io_operations_comparison_stacked_plot_log_{disk}.png'
            #plt.savefig(
            #    f'/home/michalis/Documents/UofT/MSRG/Carbon/Artemis/Supplementary_experiments/tpch/dram/plotting/changing_dram/{filename}',
            #    bbox_inches='tight', dpi=300)
            plt.savefig(
                f'/home/michalis/Documents/UofT/MSRG/Carbon/Artemis/Supplementary_experiments/tpch/dram/correct_disk_exp/plotting/{filename}',
                bbox_inches='tight', dpi=300)
            plt.close()
            print(f"Plot saved as {filename}")


def plot_IO_bytes_comparison_stacked_bar(data):
    # Plot parameters
    bar_width = 0.13
    opacity = 0.9
    group_spacing = 0.7  # Increased spacing between groups
    bar_spacing = 0.07  # Spacing between bars within a group

    for disk in disks_to_process:
        fig, ax = plt.subplots(figsize=(15, 8))
        dram_sizes = list(data[disk][databases[0]].keys())
        dram_sorted_sizes = sorted(dram_sizes, key=convert_to_gb)

        Byte_types = ['Read Bytes', 'Write Bytes']
        Byte_types = ['Write Bytes']
        colors = plt.cm.get_cmap('tab10')(np.linspace(0, 1, len(Byte_types)))

        # Calculate positions for each group and bar
        group_width = len(dram_sizes) * (bar_width + bar_spacing) - bar_spacing
        group_positions = np.arange(len(databases)) * (len(dram_sizes) * bar_width + group_spacing)

        for i, db in enumerate(databases):
            for j, dram_size in enumerate(dram_sorted_sizes):
                bottom = 1e-10  # Small non-zero value for log scale
                bar_position = group_positions[i] + j * (bar_width + bar_spacing)
                total_gbytes = 0
                for k, Byte_type in enumerate(Byte_types):
                    value = data[disk][db][dram_size][Byte_type]/(1024*1024*1024)
                    rect = ax.bar(bar_position, value, bar_width, bottom=bottom, alpha=opacity,
                                  label=f'{Byte_type}' if i == 0 and j == 0 else "",
                                  color=colors[k])
                    bottom += value
                    total_gbytes += value

                #total_mbytes = total_bytes/(1024*1024)
                ax.text(bar_position, total_gbytes, f'{int(total_gbytes)}', ha='center', va='bottom', fontsize=9,
                        rotation=45, fontweight='bold')
                ax.text(bar_position, -0.02, dram_size.upper(),
                        ha='center', va='top', fontsize=10, transform=ax.get_xaxis_transform(),rotation=45)

        ax.set_ylabel('I/O gigabytes')
        #ax.set_yscale('log')
        ax.set_title(f' Total I/0 gigabytes ', fontsize=14)
        ax.set_xticks(group_positions + group_width / 2)
        ax.set_xticklabels(databases, rotation=0, ha='right', fontweight='bold')
        # Adjust x-tick label positions to be slightly lower
        ax.tick_params(axis='x', which='major', pad=45)
        ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.tight_layout()
        filename = f'io_bytes_comparison_stacked_plot_{disk}.png'
        #plt.savefig(f'/home/michalis/Documents/UofT/MSRG/Carbon/Artemis/Supplementary_experiments/tpch/dram/plotting/changing_dram/{filename}', bbox_inches='tight')
        plt.savefig(
            f'/home/michalis/Documents/UofT/MSRG/Carbon/Artemis/Supplementary_experiments/tpch/dram/correct_disk_exp/plotting/{filename}',
            bbox_inches='tight')
        plt.close()
        print(f"Plot saved as {filename}")

        # Plot parameters
        bar_width = 0.13
        opacity = 0.9
        group_spacing = 0.7
        bar_spacing = 0.07

        for disk in disks_to_process:
            fig, ax = plt.subplots(figsize=(20, 10))  # Increased width, reduced height
            dram_sizes = list(data[disk][databases[0]].keys())
            dram_sorted_sizes = sorted(dram_sizes, key=convert_to_gb)
            Byte_types = ['Read Bytes', 'Write Bytes']
            colors = plt.cm.get_cmap('tab10')(np.linspace(0, 1, len(Byte_types)))

            group_width = len(dram_sizes) * (bar_width + bar_spacing) - bar_spacing
            group_positions = np.arange(len(databases)) * (len(dram_sizes) * bar_width + group_spacing)

            for i, db in enumerate(databases):
                for j, dram_size in enumerate(dram_sorted_sizes):
                    bottom = 1
                    bar_position = group_positions[i] + j * (bar_width + bar_spacing)
                    total_gigabytes = 0
                    for k, Byte_type in enumerate(Byte_types):
                        value = max(data[disk][db][dram_size][Byte_type], 1)/(1024*1024*1024)  # Ensure non-zero values
                        rect = ax.bar(bar_position, value, bar_width, bottom=bottom, alpha=opacity,
                                      label=f'{Byte_type}' if i == 0 and j == 0 else "",
                                      color=colors[k])
                        bottom += value
                        total_gigabytes += value


                    ax.text(bar_position, total_gigabytes, f'{int(total_gigabytes)}', ha='center', va='bottom', fontsize=7,
                            rotation=45, fontweight='bold')
                    ax.text(bar_position, -0.02, dram_size.upper(),
                            ha='center', va='top', fontsize=8, transform=ax.get_xaxis_transform(), rotation=45)

            ax.set_ylabel('I/O gigabytes (log scale)')
            ax.set_yscale('log')
            ax.set_ylim(1, ax.get_ylim()[1])  # Set lower limit to 1
            ax.set_title(f'Total I/O gigabytes', fontsize=14)
            ax.set_xticks(group_positions + group_width / 2)
            ax.set_xticklabels(databases, rotation=0, ha='right', fontweight='bold', fontsize=10)
            ax.tick_params(axis='x', which='major', pad=50)
            ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=8)

            #plt.tight_layout()
            filename = f'io_bytes_comparison_stacked_plot_log_{disk}.png'
            #plt.savefig(
             #   f'/home/michalis/Documents/UofT/MSRG/Carbon/Artemis/Supplementary_experiments/tpch/dram/plotting/changing_dram/{filename}',
             #   bbox_inches='tight', dpi=300)
            plt.savefig(
                f'/home/michalis/Documents/UofT/MSRG/Carbon/Artemis/Supplementary_experiments/tpch/dram/correct_disk_exp/plotting/{filename}',
                bbox_inches='tight', dpi=300)
            plt.close()
            print(f"Plot saved as {filename}")


def plot_endurance_comparison_per_year(data):
    # Plot parameters
    bar_width = 0.13
    opacity = 0.9
    group_spacing = 0.7  # Increased spacing between groups
    bar_spacing = 0.07  # Spacing between bars within a group
    workload_rate = 55 # terabytes per year

    for disk in disks_to_process:
        fig, ax = plt.subplots(figsize=(15, 8))

        dram_sizes = list(data[disk][databases[0]].keys())
        dram_sorted_sizes = sorted(dram_sizes, key=convert_to_gb)
        byte_types = ['Read Bytes', 'Write Bytes']
        byte_types = [ 'Write Bytes']
        colors = plt.cm.get_cmap('tab10')(np.linspace(0, 1, len(byte_types)))

        # Calculate positions for each group and bar
        group_width = len(dram_sizes) * (bar_width + bar_spacing) - bar_spacing
        group_positions = np.arange(len(databases)) * (len(dram_sizes) * bar_width + group_spacing)


        for i, db in enumerate(databases):
            for j, dram_size in enumerate(dram_sorted_sizes):
                bottom = 1e-10  # Small non-zero value for log scale
                bar_position = group_positions[i] + j * (bar_width + bar_spacing)
                total_gbytes = 0
                queries_per_year = math.ceil(24 * 60 * 60 * 365 / data[disk][db][dram_size]['Latency(sec)'])
                terabytes_gigabytes = 0
                for k,byte_type in enumerate(byte_types):
                    bytes_per_year = data[disk][db][dram_size][byte_type] * queries_per_year
                    terabytes_per_year =   bytes_per_year / (1024 * 1024 * 1024*1024) #bytes to terabytes

                    rect = ax.bar(bar_position, terabytes_per_year, bar_width, bottom=bottom, alpha=opacity,
                                  label=f'{byte_type}' if i == 0 and j == 0 else "",
                                  color=colors[k])
                    bottom += terabytes_per_year
                    terabytes_gigabytes = terabytes_gigabytes + terabytes_per_year

                ax.text(bar_position, terabytes_gigabytes, f'{int(terabytes_gigabytes)}', ha='center', va='bottom', fontsize=7,
                        rotation=45, fontweight='bold')
                ax.text(bar_position, -0.02, dram_size.upper(),
                        ha='center', va='top', fontsize=8, transform=ax.get_xaxis_transform(), rotation=45)

        ax.axhline(y=workload_rate, color='#666666', linestyle='--', alpha=0.7)
        ax.text(ax.get_xlim()[1], workload_rate, f' {workload_rate} TB/year',
                va='center', ha='left', color='#666666')
        ax.set_ylabel('I/O terabytes (log scale)')
        #ax.set_yscale('log')
        ax.set_ylim(1, ax.get_ylim()[1])  # Set lower limit to 1
        ax.set_title(f'Total I/O terabytes for 1 year of continuous operation', fontsize=14)
        ax.set_xticks(group_positions + group_width / 2)
        ax.set_xticklabels(databases, rotation=0, ha='right', fontweight='bold', fontsize=10)
        ax.tick_params(axis='x', which='major', pad=50)
        ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=8)

        filename = f'io_endurance_comparison_stacked_plot_log_{disk}.png'

        plt.savefig(
            f'/home/michalis/Documents/UofT/MSRG/Carbon/Artemis/Supplementary_experiments/tpch/dram/correct_disk_exp/plotting/{filename}',
            bbox_inches='tight', dpi=300)
        plt.close()
        print(f"Plot saved as {filename}")

def main():
    #base_directory = '/home/michalis/Documents/UofT/MSRG/Carbon/Artemis/Supplementary_experiments/tpch/dram/correct_disk_exp'
    #base_directory = '/home/michalis/Documents/UofT/MSRG/Carbon/Artemis/Supplementary_experiments/tpch/dram'
    base_directory = '/home/michalis/Documents/UofT/MSRG/Carbon/Artemis/Supplementary_experiments/tpch/ssd_hdd/ssd'

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

    plot_IO_count_comparison_stacked_bar(total_data)
    plot_IO_bytes_comparison_stacked_bar(total_data)
    plot_endurance_comparison_per_year(total_data)


if __name__ == "__main__":
    main()
import argparse
import os
import random
from collections import defaultdict

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
import math
import glob
import ast
import plotly.express as px


def collectLabelStrings():
    label_values = []

    while len(label_values) == 0:
        random_csv = random.choice(csv_files)
        df = pd.read_csv(random_csv)

        # Check if it is a valid csv file
        if not any(df['Label'] == "Throughput(queries/sec)"):
            continue

        # Get the values from the "Label" column
        label_values = df['Label'].tolist()

        # Remove values 'idle state', 'Throughput(queries/sec)', and the last value
        label_values = [value for value in label_values if
                        value not in ['idle state', 'Throughput(queries/sec)', label_values[-1]]]

    return label_values


def plotMemoryStacked(total_memory_data, scale_factor):
    colors = ['#9c575f', '#8763a3', '#c87081', '#51a09c', '#5186c4', '#32527a', '#e58879', '#c5a279', '#637b4c']
    for label, data in total_memory_data.items():
        unique_databases = data[0].index
        fig, ax = plt.subplots(round(len(unique_databases) / 2), 2, figsize=(20, 13), dpi=96, sharex=True)
        # Add in title and subtitle
        ax[0, 0].text(x=0.05, y=.93, s="Memory usage over time for " + label,
                      transform=fig.transFigure, ha='left', fontsize=14, weight='bold', alpha=.8)
        ax[0, 0].text(x=0.05, y=.90,
                      s='Scale factor ' + str(scale_factor),
                      transform=fig.transFigure, ha='left', fontsize=12, alpha=.8, weight='bold')
        counter = 0
        axes = ax.flatten()
        legend_handles = []
        legend_labels = ['Occupied Memory', 'Cached Memory']
        for i, ax in enumerate(axes):
            db_name = unique_databases[i]
            if db_name in data[0].index and db_name in data[1].index:
                values1 = data[0].loc[db_name]  # Occupied Memory
                values2 = data[1].loc[db_name]  # Cached memory
                # fig, ax = plt.subplots(figsize=(13.33, 7.5), dpi=96)
                # Calculate the step size based on the number of measurements
                step_size = max(1, 10 ** (math.ceil(math.log10(len(values1))) - 1))
                ax.set_xticks(range(0, len(values1), step_size))
                ax.set_xticklabels(values1.index[::step_size], rotation=45)
                # Adding labels and title
                if i >= len(axes) - 2:
                    ax.set_xlabel('Measurements', fontsize=12, labelpad=10)
                else:
                    ax.set_xlabel('', fontsize=12, labelpad=10)
                ax.set_ylabel('Memory Usage', fontsize=12, labelpad=10)
                ax.set_title(db_name.capitalize())
                ax.grid(which="major", axis='x', linestyle='--', color='#5b5b5b', alpha=0.2, zorder=1)
                ax.grid(which="major", axis='y', linestyle='--', color='#5b5b5b', alpha=0.2, zorder=1)
                ax.spines[['top', 'right', 'bottom']].set_visible(False)
                # Make the left spine thicker
                ax.spines['left'].set_linewidth(1.1)
                # Add in red line and rectangle on top
                ax.plot([0.05, .90], [.98, .98], transform=fig.transFigure, clip_on=False, color='#E3120B',
                        linewidth=.6)
                ax.add_patch(plt.Rectangle((0.05, .98), 0.04, -0.02, facecolor='#E3120B', transform=fig.transFigure,
                                           clip_on=False, linewidth=0))

                ax.stackplot(values1.index, values1, values2,
                             labels=[f'Occupied Memory', f'Cached Memory'], colors=colors,
                             alpha=0.9)
                # ax.legend(loc='best')
                handles, _ = ax.get_legend_handles_labels()
                legend_handles.extend(handles)
                counter = counter + 1

        # Adjust the margins around the plot area
        plt.subplots_adjust(left=None, bottom=0.2, right=None, top=0.85, wspace=0.3, hspace=0.5)
        # Create a single legend outside the subplots
        fig.legend(legend_handles, legend_labels, loc='upper center', fontsize='large', bbox_to_anchor=(0.5, 0.90),
                   ncol=len(legend_labels))

        # Set a white background
        fig.patch.set_facecolor('white')

        # fig.show()
        plot_path = os.path.join(plot_dir, os.path.join(plot_dir,
                                                        f"queries/total-memory_{benchmark}_{scale_factor}_{label}.png"))
        plt.savefig(plot_path)
        plt.close()


def plotMemoryOverTime(scale_factors):
    label_values = collectLabelStrings()

    for scale_factor in scale_factors:
        # Dictionary that stores the dataframe for cached and used memory for plotting
        total_memory_data = defaultdict(list)
        columns_to_plot = {
            'dram_usage(GB)': 'Memory Usage',
            'dram_cache(GB)': 'Disk Cache Usage'
        }
        for column in columns_to_plot.keys():
            for label in label_values:
                # Dictionary to store memory values for each CSV file
                memory_data = pd.Series()

                # The labels of the memory values
                database_indexes = []

                for csv_file in csv_files:
                    df = pd.read_csv(csv_file)
                    if not any(df['Label'] == "Throughput(queries/sec)"):
                        continue
                    split_elements = csv_file.stem.split("_")
                    bm = split_elements[0]
                    sf = split_elements[1]
                    if bm != benchmark or sf != scale_factor:
                        continue
                    database = split_elements[2] if len(split_elements) == 3 else split_elements[2] + "_" + \
                                                                                  split_elements[
                                                                                      3]
                    label_value = df[df['Label'] == label]

                    try:
                        memory_data = pd.concat([memory_data, label_value[column]],
                                                ignore_index=True)
                    except KeyError:
                        continue
                    database_indexes.append(database)
                memory_data.index = database_indexes

                memory_df = pd.DataFrame(index=memory_data.index)
                # Convert list-like string to a list for each value in the Series
                for idx, value in memory_data.items():
                    memory_data[idx] = ast.literal_eval(value)

                # Iterate over each index-label pair in the Series
                for database, values in memory_data.items():
                    # Iterate over each element in the list
                    for i, value in enumerate(values):
                        # Create column name based on the index and position in the list
                        column_name = f'{i}'
                        # Assign the value to the corresponding index and column
                        memory_df.at[database, column_name] = value

                memory_df.index.name = 'Databases'
                memory_df.columns.name = columns_to_plot[column]

                total_memory_data[label].append(memory_df)

                # Plot
                for index, row in memory_df.iterrows():
                    plt.plot(row.dropna(), label=index)  # Drop NaN values and plot the line

                # Add labels and title
                plt.xlabel('Measurements')
                plt.ylabel(columns_to_plot[column] + "(GB)")
                plt.title(columns_to_plot[column] + ' Over time - ' + label)

                # Count non-NaN values in each row
                row_counts = memory_df.count(axis=1)

                # Find the row with the largest number of non-NaN values
                max_row = row_counts.idxmax()
                # Calculate the step size based on the number of measurements
                step_size = max(1, 10 ** (math.ceil(math.log10(row_counts[max_row])) - 1))

                plt.xticks(range(0, len(memory_df.columns), step_size), memory_df.columns[::step_size], rotation=45)

                # Add legend
                plt.legend(bbox_to_anchor=(1.02, 1.0), loc='upper left')

                plt.tight_layout()

                plot_path = os.path.join(plot_dir, os.path.join(plot_dir,
                                                                f"queries/{column.replace('(GB)', '')}_{benchmark}_{scale_factor}_{label}.png"))
                plt.savefig(plot_path)
                plt.close()

        plotMemoryStacked(total_memory_data, scale_factor)


def plotCPU(scale_factors):
    label_values = collectLabelStrings()

    for scale_factor in scale_factors:
        columns_to_plot = {
            'cpu_utilization(%)': 'CPU Utilization'
        }
        for column in columns_to_plot.keys():
            for label in label_values:

                # Dictionary to store CPU utilization values for each CSV file
                cpu_utilization_data = pd.Series()

                # The labels of the cpu_utilization_data
                database_indexes = []

                for csv_file in csv_files:
                    df = pd.read_csv(csv_file)
                    if not any(df['Label'] == "Throughput(queries/sec)"):
                        continue
                    split_elements = csv_file.stem.split("_")
                    bm = split_elements[0]
                    sf = split_elements[1]
                    if bm != benchmark or sf != scale_factor:
                        continue
                    database = split_elements[2] if len(split_elements) == 3 else split_elements[2] + "_" + \
                                                                                  split_elements[
                                                                                      3]
                    label_value = df[df['Label'] == label]

                    try:
                        cpu_utilization_data = pd.concat([cpu_utilization_data, label_value['cpu_utilization(%)']],
                                                         ignore_index=True)
                    except KeyError:
                        continue
                    database_indexes.append(database)
                cpu_utilization_data.index = database_indexes

                thread_df = pd.DataFrame(index=cpu_utilization_data.index)

                # Convert list-like string to a list for each value in the Series
                for idx, value in cpu_utilization_data.items():
                    cpu_utilization_data[idx] = ast.literal_eval(value)

                # Iterate over each index-label pair in the Series
                for database, values in cpu_utilization_data.items():
                    # Iterate over each element in the list
                    for i, value in enumerate(values):
                        # Create column name based on the index and position in the list
                        column_name = f'thread {i + 1}'
                        # Assign the value to the corresponding index and column
                        thread_df.at[database, column_name] = value

                thread_df.index.name = 'Databases'
                thread_df.columns.name = 'Threads'

                # Plot the DataFrame using the plot() method
                ax = thread_df.T.plot(kind='bar', figsize=(14, 10))

                # Set labels and title
                ax.set_xlabel('Threads')
                ax.set_ylabel('Utilization %')
                ax.set_title('CPU Utilization - ' + label)
                ax.set_xticklabels(thread_df.T.index, rotation=45)

                plt.close()

                fig = px.scatter(thread_df, color="Threads", title="CPU Utilization - " + label,
                                 labels={"value": "CPU Utilization(%)"}, height=600)

                if not os.path.exists(os.path.join(plot_dir, "queries")):
                    os.mkdir(os.path.join(plot_dir, "queries"))
                fig.write_image(
                    os.path.join(plot_dir, os.path.join(plot_dir,
                                                        f"queries/{column.replace('(%)', '')}_{benchmark}_{scale_factor}_{label}.png")),
                    scale=6)


def plotEnergy(scale_factors):
    for scale_factor in scale_factors:
        df_columns = [benchmark + '-' + scale_factor]

        columns_to_plot = {
            'dram_totalE(J)': 'DRAM Energy',
            'cpu_0_totalE(J)': 'CPU Energy',
            'Latency(sec)': 'Query latency',
            'Read Count': 'Read I/O',
            'Write Count': 'Write I/O'
        }
        for column in columns_to_plot.keys():
            # For energy consumption plot in log scale
            log_scale = False
            if "E(J)" in column:
                log_scale = True
            data_to_plot_dict = {}
            load_data_to_plot_dict = {}
            df_columns = []
            for csv_file in csv_files:
                split_elements = csv_file.stem.split("_")
                bm = split_elements[0]
                sf = split_elements[1]
                if bm != benchmark or sf != scale_factor:
                    continue
                database = split_elements[2] if len(split_elements) == 3 else split_elements[2] + "_" + split_elements[
                    3]
                df = pd.read_csv(csv_file).fillna(0)
                # Check if all the CSV files contain actual data(some executions may have been stopped)
                if not any(df['Label'] == "Throughput(queries/sec)"):
                    continue
                df_columns.append(database)
                df_filtered = df[['Label', column]]
                # Exclude idle state and throughput values
                for index, row in df.iloc[1:-2].iterrows():
                    key = row['Label']
                    if log_scale:
                        if row[column] == 0:
                            value = 0
                        else:
                            # Logarithmic scale per 1000 queries
                            value = math.log10(row[column] * 1000)
                    else:
                        value = row[column]
                    if "load data" in key:
                        if key in load_data_to_plot_dict:
                            load_data_to_plot_dict[key].append(value)
                        else:
                            load_data_to_plot_dict[key] = [value]
                        continue
                    if key in data_to_plot_dict:
                        # If it is not the first iteration
                        data_to_plot_dict[key].append(value)
                    else:
                        # If it is the first iteration
                        data_to_plot_dict[key] = [value]

            # Plotting

            load_data_df_plot = pd.DataFrame.from_dict(load_data_to_plot_dict).T
            if load_data_df_plot.empty:
                print("The DataFrame for loading data is empty for " + column + " and scale factor " + scale_factor)
                continue
            load_data_df_plot = load_data_df_plot.reset_index()
            load_data_df_plot.set_index(load_data_df_plot.columns[0], inplace=True)
            load_data_df_plot.index.name = 'Queries'
            load_data_df_plot.columns = df_columns
            load_data_df_plot.columns.name = 'Databases'
            title = benchmark + ' ' + scale_factor
            ax = load_data_df_plot.plot(kind='bar',
                                        stacked=False,
                                        title=title.upper() + ' ' + columns_to_plot[column], figsize=(14, 10))
            ax.set_ylabel(column)
            ax.set_xticklabels(load_data_df_plot.index, rotation=45)
            ax.set_xlabel('')
            # if log_scale:
            #   ax.set_yscale('log')
            plt.tight_layout()
            plot_path = os.path.join(plot_dir,
                                     benchmark + "_" + scale_factor + "_" + "Load_" + columns_to_plot[column].replace(
                                         " ",
                                         "").replace(
                                         "/", ""))
            plt.savefig(plot_path)
            # plt.show()
            plt.close()

            fig = px.scatter(load_data_df_plot, color="Databases", title=title.upper() + ' ' + columns_to_plot[column],
                             labels={"value": columns_to_plot[column]}, height=600)

            if not os.path.exists(plot_dir):
                os.mkdir(plot_dir)
            fig.write_image(plot_path + "_bubble.png", scale=6)

            queries_df = pd.DataFrame.from_dict(data_to_plot_dict).T
            if queries_df.empty:
                print("The DataFrame is empty for " + column + " and scale factor " + scale_factor)
                continue
            queries_df = queries_df.reset_index()
            queries_df.set_index(queries_df.columns[0], inplace=True)
            queries_df.index.name = 'Queries'
            queries_df.columns = df_columns
            queries_df.columns.name = 'Databases'
            title = benchmark + ' ' + scale_factor
            ax = queries_df.plot(kind='bar',
                                 stacked=False,
                                 title=title.upper() + ' ' + columns_to_plot[column], figsize=(14, 10))
            ax.set_ylabel(column)
            ax.set_xticks(range(len(queries_df.index)))  # Set tick locations
            ax.set_xticklabels(queries_df.index, rotation=45, ha='right')  # Set tick labels
            ax.set_xlabel('Queries', fontweight='bold')
            # if log_scale:
            #    ax.set_yscale('log')
            plt.tight_layout()
            plot_path = os.path.join(plot_dir,
                                     benchmark + "_" + scale_factor + "_" + columns_to_plot[column].replace(
                                         " ",
                                         "").replace(
                                         "/", ""))
            plt.savefig(plot_path)
            # plt.show()
            plt.close()

            fig = px.scatter(queries_df, color="Databases", title=title.upper() + ' ' + columns_to_plot[column],
                             labels={"value": columns_to_plot[column]}, height=600)
            fig.update_traces(marker=dict(size=10))  # Adjust the size here

            if not os.path.exists(plot_dir):
                os.mkdir(plot_dir)
            fig.update_xaxes(tickangle=-45)
            fig.write_image(plot_path + "_bubble.png", scale=6)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Plot benchmark results")
    parser.add_argument("--csv_path", type=str, help="path of csv results", required=True)
    parser.add_argument("--benchmark", type=str, default=1, choices=["tpch", "tpcds", "ssb"],
                        help="Choose between tpch,tpcds,ssb", required=True)
    args = parser.parse_args()
    benchmark = args.benchmark

    csv_dir = Path(args.csv_path)
    csv_files = list(csv_dir.glob("*.csv"))
    plot_dir = os.path.join(csv_dir, 'plots/')
    if not os.path.exists(plot_dir):
        os.mkdir(plot_dir)

    scale_factors = ["20gb"]
    plotEnergy(scale_factors)
    plotCPU(scale_factors)
    plotMemoryOverTime(scale_factors)

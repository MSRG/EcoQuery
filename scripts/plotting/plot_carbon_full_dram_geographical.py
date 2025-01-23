import os
import re
import glob
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter
import matplotlib
matplotlib.use('Qt5Agg')
import calendar
import seaborn as sns
import numpy as np

import plotly.graph_objects as go

# Set the style for seaborn
sns.set_style("whitegrid")
plt.rcParams['font.family'] = 'DejaVu Sans'


# List of databases
databases = ['DuckDB', 'Hyper', 'Starrocks', 'MonetDB']

zone_labels = {
    'California Independent System Operator': 'California',
    'Florida Municipal Power Pool': 'Florida',
    'Nevada Power Company': 'Nevada'
}

scale_factor = 1000 # Scale factor

# Function to extract all unique columns from a dataframe
def extract_all_columns(df):
    return df.columns.tolist()


def read_geographical_codes(file_path):
    """Read accepted codes from a file."""
    with open(file_path, 'r') as file:
        return [line.strip() for line in file if not line.startswith('#')]


def create_matching_pattern(accepted_codes):
    """Create a regex pattern for exact code matching."""
    code_patterns = []
    for code in accepted_codes:
        escaped_code = re.escape(code)
        code_patterns.append(f"{escaped_code}(?:$|[-_][^-_]+(?:-\\d+|_\\d+)?$)")

    return r'^(' + '|'.join(code_patterns) + r')$'


def process_carbon_data_files(data_folder, geographical_codes):
    """Process data files that match the accepted codes."""
    all_data = []

    pattern = create_matching_pattern(geographical_codes)

    for filename in os.listdir(data_folder):
        base_filename = os.path.splitext(filename)[0]
        file_code = base_filename.split('_')[0]
        if file_code in geographical_codes:
            file_path = os.path.join(data_folder,filename)
            try:
                df = pd.read_csv(file_path)
                df['Filename'] = filename
                all_data.append(df)
            except Exception as e:
                print(f"Error reading file {filename}: {e}")

    if not all_data:
        print("No matching files found or all files were empty.")
        return pd.DataFrame()  # Return an empty DataFrame if no data

    return pd.concat(all_data,ignore_index=True)

def aggregate_metrics(directory, query_data,load_data):
    columns_to_process = ['dram_totalE(J)', 'cpu_totalE(J)']


    # Loop through each database
    for db in databases:
        # Initialize dictionary for the database if not already present
        if db not in query_data:
            query_data[db] = {}
            load_data[db] = {}
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
            # df_filtered = df[df['Label'].str.contains('tpch')]
            df_filtered = df.iloc[:-2]  # Exclude the last two rows
            df_filtered = df_filtered.iloc[2:]  # Exclude the first two rows(idle state and load)
            # Get all unique columns dynamically
            all_columns = extract_all_columns(df_filtered)

            # Total values of selected metrics for all the queries' benchmark
            sum_queries_values = df_filtered[columns_to_process].sum()
            load_values = df[columns_to_process].iloc[1]

            for metric, load_value in load_values.items():
                if metric not in load_data[db]:
                    load_data[db][metric] = 0
                    load_data[db][metric] = load_data[db][metric] + load_value
                load_data[db][metric] = (load_data[db][metric] + load_value) / 2  # Running average of total metric for all experiments(load operation)



            for metric, sum_value in sum_queries_values.items():
                if metric not in query_data[db]:
                    query_data[db][metric] = 0
                    query_data[db][metric] = query_data[db][metric] + sum_value
                query_data[db][metric] = (query_data[db][metric] + sum_value) / 2  # Running average of total metric for all experiments(queries)


def collect_energy_data(base_directory):
    # Initialize the dictionary to store energy query_average_total
    query_average_total = {} # Average values of all experiments of the total energy consumption of the benchmark's queries
    load_average_total = {} # Average values of all experiments of the total energy consumption of the benchmark's load operation

    # Traverse each Round directory
    for round_folder in os.listdir(base_directory):
        if round_folder.startswith("Round"):
            round_directory = os.path.join(base_directory, round_folder)
            if os.path.isdir(round_directory):
                aggregate_metrics(round_directory,query_average_total,load_average_total)

    return {'Query data':query_average_total,'Load data': load_average_total}

# Function to calculate carbon emissions
def calculate_emissions(energy, carbon_intensity):
    return energy * carbon_intensity*scale_factor / (3.6 * 10 ** 6)  # Convert to gCO2e for scale_factor executions scale

# Create a plot for each database and for each year
def create_plot_db_and_year(energy_data,carbon_data):
    # Color palette
    custom_palette = [
        '#e6194B', '#3cb44b', '#ffe119', '#4363d8', '#f58231', '#911eb4', '#42d4f4', '#f032e6',
        '#bfef45', '#fabed4', '#469990', '#dcbeff', '#9A6324', '#fffac8', '#800000', '#aaffc3',
        '#808000', '#ffd8b1', '#000075', '#a9a9a9', '#ffffff', '#000000'
    ]

    # List of month names in order
    month_names = [calendar.month_name[i] for i in range(1, 13)]

    for operation, data in energy_data.items():
        for db_name, db_data in data.items():
            metrics = ['dram_totalE(J)', 'cpu_totalE(J)', 'total_energy']
            db_data['total_energy'] = db_data['dram_totalE(J)'] + db_data['cpu_totalE(J)']
            for metric in metrics:
                energy = db_data[metric]
                # Calculate emissions for each country and year
                for year in carbon_data['Year'].unique():
                    year_data = carbon_data[carbon_data['Year'] == year]
                    fig, ax = plt.subplots(figsize=(16, 9))  # 16:9 aspect ratio

                    # Get unique combinations of Country and Zone Name
                    country_zones = year_data.groupby(['Country', 'Zone Name']).size().reset_index()[
                        ['Country', 'Zone Name']]

                    def create_label(row):
                        if row['Country'] == row['Zone Name']:
                            return row['Country']
                        elif row['Zone Name'] in zone_labels:
                            return f"{row['Country']} - {zone_labels[row['Zone Name']]}"
                        else:
                            return f"{row['Country']} - {row['Zone Name']}"

                    country_zones['Label'] = country_zones.apply(create_label, axis=1)

                    print(f"Country-Zone combinations: {country_zones.to_dict('records')}")

                    # Generate a color palette based on the number of unique labels
                    # color_palette = plt.cm.get_cmap('Set1')(np.linspace(0, 1, len(country_zones['Label'].unique())))
                    # print(f"Color palette length: {len(color_palette)}")

                    for idx, (_, row) in enumerate(country_zones.iterrows()):
                        country = row['Country']
                        zone = row['Zone Name']
                        label = row['Label']
                        print(f"Processing: {label}, index: {idx}")

                        country_zone_data = year_data[
                            (year_data['Country'] == country) & (year_data['Zone Name'] == zone)]
                        country_zone_data = country_zone_data.sort_values('Month')
                        emissions = country_zone_data.apply(
                            lambda row: calculate_emissions(energy, row['Carbon Intensity gCO₂eq/kWh (LCA)']), axis=1)

                        country_zone_data['MonthName'] = pd.Categorical(country_zone_data['MonthName'],
                                                                        categories=month_names, ordered=True)

                        try:
                            ax.plot(country_zone_data['MonthName'].astype(str), emissions.tolist(),
                                    label=label, color=custom_palette[idx % len(custom_palette)], linewidth=2.5,
                                    marker='o')
                        except Exception as e:
                            print(f"Error plotting for {label}: {e}")
                            print(f"Color palette index: {idx % len(custom_palette)}")
                            print(f"Color value: {custom_palette[idx % len(custom_palette)]}")

                    metric_filename = "CPU+DRAM" if metric == 'total_energy' else metric.split("_")[0]
                    ax.set_title(
                        f'{db_name} - {metric_filename} Carbon Emissions ({year}) - {scale_factor} executions',
                        fontsize=20,
                        fontweight='bold', pad=20)
                    ax.set_xlabel('Month', fontsize=16, labelpad=5,fontweight='bold')
                    ax.set_ylabel('Carbon Emissions (gCO₂e)', fontsize=16, labelpad=10)

                    ax.set_xticks(month_names)
                    ax.set_xticklabels(month_names, rotation=45, ha='right',fontsize=16)
                    ax.set_xlim(month_names[0], month_names[-1])

                    # Add light grey vertical lines for each month
                    for month in month_names:
                        ax.axvline(x=month, color='lightgrey', linestyle='--', linewidth=0.5, zorder=0)

                    # Customize grid
                    ax.grid(axis='y', linestyle='--', alpha=0.7)
                    ax.tick_params(axis='y', labelsize=14)

                    # Move legend outside the plot, to the top right
                    ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=14, title='Country',
                              title_fontsize=16)

                    # Remove top and right spines
                    ax.spines['top'].set_visible(False)
                    ax.spines['right'].set_visible(False)

                    # Add subtle background color
                    ax.set_facecolor('#f8f8f8')

                    plt.tight_layout()
                    plt.savefig(
                        f'/home/michalis/Documents/UofT/MSRG/Carbon/Artemis/Supplementary_experiments/tpch/dram/plotting/full_dram_geographical/{db_name}_{metric.split("_")[0]}_{year}_{operation.lower()}_emissions.png',
                        dpi=500,bbox_inches='tight')
                    # plt.savefig(f'{db_name}_{metric.split("_")[0]}_{year}_emissions.png')
                    plt.close()



def create_visualization(processed_data):
    # Select specific locations
    selected_locations = ['Cyprus', 'Québec', 'Ontario', 'Germany', 'France']
    # Get data for August (month 8)
    august_data = processed_data[processed_data['Month'] == 8]
    august_data = august_data[august_data['Location'].isin(selected_locations)]

    # Sort locations by average emissions (to show clear progression)
    location_order = (august_data.groupby('Location')['Emissions']
                      .mean()
                      .sort_values(ascending=True)
                      .index.tolist())

    # Create figure
    fig = go.Figure()

    # Add bars for each database
    #databases = processed_data['Database'].unique()
    colors = {'DuckDB': '#22c55e', 'Hyper': '#3b82f6',
              'MonetDB': '#ef4444', 'Starrocks': '#f97316'}

    for db in databases:
        db_data = august_data[august_data['Database'] == db]

        # Sort the data according to our location order
        db_data = db_data.set_index('Location').reindex(location_order).reset_index()

        fig.add_trace(go.Bar(
            name=db,
            x=db_data['Location'],
            y=db_data['Emissions'],
            marker_color=colors[db]
        ))

        # Update layout
    fig.update_layout(
        title={
            'text': 'Carbon Emissions by Location and Database System',
            'y': 0.95,
            'x': 0.5,
            'xanchor': 'center',
            'yanchor': 'top',
            'font': dict(size=24)  # Larger title font
        },
        xaxis=dict(
            title='Location',
            title_font=dict(size=22,family='Arial Bold'),  # Larger x-axis title
            tickfont=dict(size=20)  # Larger x-axis tick labels
        ),
        yaxis=dict(
            title='Carbon Emissions (gCO₂e) - Log Scale',
            title_font=dict(size=22),  # Larger y-axis title
            tickfont=dict(size=20),  # Larger y-axis tick labels
            type='log'
        ),
        barmode='group',
        height=800,  # Increased height
        width=1200,  # Increased width
        showlegend=True,
        template='plotly_white',
        legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="right",
            x=1.1,
            font=dict(size=22)  # Larger legend font
        )
    )

    # Add annotations
    fig.add_annotation(
        text="Carbon emissions (gCO2e) for 1000 TPC-H executions - August 2022",
        xref="paper", yref="paper",
        x=0, y=0.95,
        showarrow=False,
        font=dict(size=20)
    )

    # Update bar width and spacing
    fig.update_layout(
        bargap=0.15,  # Gap between bars in the same group
        bargroupgap=0.1  # Gap between bar groups
    )

    return fig

# Plot the databases' emissions for different locations for a specific month
def plot_location_comparison(energy_data,carbon_data):
    query_energy = energy_data['Query data']

    # Calculate total energy (CPU + DRAM) for each database
    db_energy = {
        db: data['cpu_totalE(J)'] + data['dram_totalE(J)'] for db, data in query_energy.items()
    }

    # Process carbon intensity data
    carbon_df = carbon_data.copy()
    carbon_df['Month'] = pd.to_datetime(carbon_df['Datetime (UTC)']).dt.month
    carbon_df['Year'] = pd.to_datetime(carbon_df['Datetime (UTC)']).dt.year

    # Filter for 2022 data
    carbon_df = carbon_df[carbon_df['Year'] == 2022]

    # Calculate emissions for each location and database
    results = []

    for _, row in carbon_df.iterrows():
        carbon_intensity = row['Carbon Intensity gCO₂eq/kWh (direct)']
        location = row['Zone Name']
        month = row['Month']

        for db_name, energy in db_energy.items():
            emissions = calculate_emissions(energy, carbon_intensity)
            results.append({
                'Location': location,
                'Month': month,
                'Database': db_name,
                'Emissions': emissions
            })

    processed_data = pd.DataFrame(results)

    # Create and show the visualization
    fig = create_visualization(processed_data)
    fig.show()

    fig.write_image(f'/home/michalis/Documents/UofT/MSRG/Carbon/Artemis/Supplementary_experiments/tpch/dram/plotting/full_dram_geographical/location_comparison_total_emissions.png',
                    width=1920, height=1080,  # Full HD resolution
                    scale=2)




def main():
    energy_directory = '/home/michalis/Documents/UofT/MSRG/Carbon/Artemis/Supplementary_experiments/tpch/dram/dram_512gb_sf_300gb'
    carbon_directory = '/home/michalis/Documents/UofT/MSRG/Carbon/electricity-maps-data/Monthly'
    geographical_codes_file = 'geographical_codes.txt'

    geographical_codes = read_geographical_codes(geographical_codes_file)
    carbon_data = process_carbon_data_files(carbon_directory,geographical_codes)

    carbon_data['Datetime (UTC)'] = pd.to_datetime(carbon_data['Datetime (UTC)'])
    carbon_data['Year'] = carbon_data['Datetime (UTC)'].dt.year
    carbon_data['Month'] = carbon_data['Datetime (UTC)'].dt.month
    carbon_data['MonthName'] = carbon_data['Datetime (UTC)'].dt.strftime('%B')
    #carbon_data = carbon_data.sort_values('Datetime (UTC)')

    energy_data = collect_energy_data(energy_directory)


    create_plot_db_and_year(energy_data,carbon_data)

    plot_location_comparison(energy_data, carbon_data)


    print("aaaa")




if __name__ == "__main__":
    main()
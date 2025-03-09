# EcoQuery: Sustainability Benchmark for Databases
EcoQuery and ATLAS for assessing the environmental impact of analytical databases et al.

EcoQuery is a benchmarking tool designed to measure the environmental impact of different database systems. It currently supports benchmarking with TPC-H and TPC-DS for DuckDB, Hyper, StarRocks, and MonetDB.

## Overview

EcoQuery allows you to:
- Compare the energy consumption and environmental impact of different database systems
- Benchmark using industry-standard TPC-H and TPC-DS workloads
- Easily deploy and configure multiple database systems for testing

## Database Installation

### StarRocks

1. Download the StarRocks binary distribution package:
   ```bash
   wget https://releases.starrocks.io/starrocks/StarRocks-3.1.4.tar.gz

2. Extract the package:
    ```bash
    tar -xzvf StarRocks-3.1.4.tar.gz
3.Configure the installation path in the benchmark scripts (details below).

### MonetDB

1. Follow the installation instructions at the official MonetDB repository:
   ```bash
   # Add the MonetDB repository
   sudo apt-get install -y apt-transport-https
   wget -O - https://www.monetdb.org/downloads/MonetDB-GPG-KEY | gpg --dearmor | sudo tee /etc/apt/trusted.gpg.d/monetdb.gpg > /dev/null
   sudo add-apt-repository "deb https://www.monetdb.org/downloads/deb/ $(lsb_release -cs) monetdb"
   
   # Update and install
   sudo apt-get update
   sudo apt-get install -y monetdb5-server monetdb-client

2. Configure the MonetDB dbfarm path in the benchmark scripts (details below).
### DuckDB

1. Install DuckDB using pip:
   ```bash
   pip install duckdb
2. For additional installation options, refer to the DuckDB Python installation guide.

### Hyper

1. Install Hyper using pip:
   ```bash
   pip install tableauhyperapi
2. For additional information, refer to the Hyper installation documentation.

## Running Benchmarks

### TPC-H Benchmark

To run the TPC-H benchmark, use the following command:

    ./run_tpch.sh {data_directory} {dbgen_tool_directory} {python_code_directory}   {data_generation_flag}

### Parameters

- **`data_directory`**: Directory to store generated data files.
- **`dbgen_tool_directory`**: Directory containing the TPC-H dbgen tool.
- **`python_code_directory`**: Directory containing the EcoQuery Python scripts.
- **`data_generation_flag`**: Set to `true` to generate new data, or `false` to use existing data.

### TPC-DS Benchmark

To run the TPC-DS benchmark, use the following command:

    ./run_tpcds.sh {data_directory} {dbgen_tool_directory} {python_code_directory}   {data_generation_flag}

### Parameters

- **`data_directory`**: Directory to store generated data files.
- **`dbgen_tool_directory`**: Directory containing the TPC-H dbgen tool.
- **`python_code_directory`**: Directory containing the EcoQuery Python scripts.
- **`data_generation_flag`**: Set to `true` to generate new data, or `false` to use existing data.

## Configuration

### Database Paths

Before running the benchmarks, you need to configure the paths for the databases in the script files:

1. For MonetDB, define the dbfarm path in the ile=/etc/systemd/system/tpch_benchmark.env file, which holds the variables of service files:
   ```bash
   # Example: Modify this line in the script
   DATA_DIR="/path/to/monetdb/dbfarm"
2.For StarRocks, define the installation path:
   ```bash
   # Example: Modify this line in the script
local starrocks_path=="/path/to/starrocks/installation"

#!/bin/bash


# Function to run dbgen script
run_dbgen() {
  scale=$1
  destination=$2
  run_dbgen_path=$3

  # Convert scale factor
  if [ "$scale" == "100mb" ]; then
    echo "Creating data for SF $scale" 
    scale_value=0.1
  elif [ "$scale" == "1gb" ]; then
    echo "Creating data for SF $scale"
    scale_value=1
  elif [ "$scale" == "10gb" ]; then
    echo "Creating data for SF $scale"
    scale_value=10
   elif [ "$scale" == "20gb" ]; then
    echo "Creating data for SF $scale"
    scale_value=20
   elif [ "$scale" == "50gb" ]; then
    echo "Creating data for SF $scale"
    scale_value=50
   elif [ "$scale" == "100gb" ]; then
    echo "Creating data for SF $scale"
    scale_value=100
   elif [ "$scale" == "150gb" ]; then
    echo "Creating data for SF $scale"
    scale_value=150
   elif [ "$scale" == "200gb" ]; then
    echo "Creating data for SF $scale"
    scale_value=200
   elif [ "$scale" == "300gb" ]; then
    echo "Creating data for SF $scale"
    scale_value=300
  else
    echo "Unsupported scale factor: $scale"
    exit 1
  fi

  #"$dbgen" $scale_value $destination
  "${run_dbgen_path}/run_dbgen.sh" $scale_value $destination $run_dbgen_path
}

# Function to run python benchmark script
run_benchmark() {
  data_dir=$1
  database_type=$2
  db_path=$3
  python_script_path=$4
  #Set of memory limits to try when the normal execution is killed due to OOM
  memory_limits=(5 10 15 20)
  
  # Save the current working directory
  original_dir=$(pwd)

  # Change the working directory to a new location
  cd $python_script_path

  # Perform operations in the new directory
  echo "Now in: $(pwd)"
  
  # Empty the memory caches for cold start
  sudo sh -c 'sync && echo 3 > /proc/sys/vm/drop_caches'

  case $database_type in
    "duckdb")
      sudo sh -c 'sync && echo 3 > /proc/sys/vm/drop_caches'
      python3 main.py --benchmark tpch --database duckdb --data $data_dir --db_path $db_path
      if [ "$?" -ne 0 ]; then
        echo "DuckDB is OOM"
        memory_info=$(free -m)
        # Extract the available memory value (in megabytes)
	available_memory=$(echo "$memory_info" | awk 'NR==2{print $7}')
	# Convert megabytes to gigabytes
	available_memory_gb=$(printf "%.0f" $(echo "scale=2; $available_memory / 1024" | bc))
	echo "Run DuckDB with all available memory $available_memory_gb"
	sudo sh -c 'sync && echo 3 > /proc/sys/vm/drop_caches'
	python3 main.py --benchmark tpch --database duckdb --data $data_dir --db_path $db_path --mem_limit $available_memory_gb
        for mem_limit in "${memory_limits[@]}"; do
          echo "Run DuckDB with memory limit $mem_limit"
          sudo sh -c 'sync && echo 3 > /proc/sys/vm/drop_caches'
          python3 main.py --benchmark tpch --database duckdb --data $data_dir --db_path $db_path --mem_limit $mem_limit
        done
      fi
      ;;
    "hyper")
      sudo sh -c 'sync && echo 3 > /proc/sys/vm/drop_caches'
      python3 main.py --benchmark tpch --database hyper --data $data_dir --db_path $db_path
      ;;
    "monetdb")
      sudo sh -c 'sync && echo 3 > /proc/sys/vm/drop_caches'
      check_and_create_monetdb_files_dir "$db_path"
      python3 main.py --benchmark tpch --database monetdb --data $data_dir --dbname tpch --dbfarm "${db_path}/monetdb_files"
      ;;
    "starrocks")
     #Define the path for Starrocks files !!!!!!!!!!!!!!
      starrocks_path="/home/michalis/Projects/Environmental/Carbon/databases/StarRocks-3.1.4"
      cd $starrocks_path
      #Start Starrocks server
      cd "fe/bin"
      ./start_fe.sh --daemon
      sleep 5
      cd ../../be/bin/
      ./start_be.sh --daemon
      sleep 7
      cd $python_script_path
      sudo sh -c 'sync && echo 3 > /proc/sys/vm/drop_caches'
      python3 main.py --benchmark tpch --database starrocks --ip 127.0.0.1 --data $data_dir --name tpch
      cd $starrocks_path
      #Stop Starrocks server
      ./be/bin/stop_be.sh --daemon
      ./fe/bin/stop_fe.sh --daemon
      sleep 1
      # Delete data from database outside python script
      rm -rf "$starrocks_path/be/storage/data"/*
      cd $python_script_path
      ;;
    *)
      echo "Unsupported database type: $database_type"
      exit 1
      ;;
  esac
  
  # Change back to the original directory
  cd "$original_dir"

  # Perform operations in the original directory
  echo "Back to: $(pwd)"
  
}


# Function to check and create MonetDB files directory
check_and_create_monetdb_files_dir() {
  directory_path=$1
  monetdb_files_dir="${directory_path}/monetdb_files"

  if [ ! -d "$monetdb_files_dir" ]; then
    echo "Creating MonetDB files directory: $monetdb_files_dir"
    mkdir "$monetdb_files_dir"
  fi
}


# Function to check and create data directory
check_and_create_data_dir() {
  scale=$1
  directory_path=$2
  data_dir="${directory_path}/data_${scale}"

  if [ ! -d "$data_dir" ]; then
    echo "Creating data directory: $data_dir"
    mkdir "$data_dir"
  fi
}


# Main script
# Define the path to the virtual environment !!!!!!!!!!!!!!!!!!!!!!
venv_path="/home/michalis/Projects/Environmental/Carbon/venv/bin/activate"

# Activate the virtual environment
source "$venv_path"

# Check if the correct number of arguments is provided
if [ "$#" -ne 4 ]; then
  echo "Usage: $0 <directory_path> <run_dbgen_script> <python_script> <data_generation_flag>" 
  exit 1
fi

directory_path=$1
run_dbgen_path=$2
python_script_path=$3
data_gen_flag=$4

# Give permissions for RAPL
sudo chmod -R a+r /sys/class/powercap/intel-rapl

# Define an array of scale factors
scale_factors=( "1gb" "10gb" "20gb")

# Define an array of database types
databases=("duckdb" "hyper" "monetdb" "starrocks")
#databases=( "starrocks")

if [ "$4" == "true" ]; then
  # Iterate over the scale factors
  for scale_factor in "${scale_factors[@]}"; do
    check_and_create_data_dir $scale_factor "$directory_path"
    run_dbgen $scale_factor "${directory_path}/data_${scale}" $run_dbgen_path
  done
fi
# Iterate over the databases
for database_type in "${databases[@]}"; do
  # Iterate over the scale factors
  for scale_factor in "${scale_factors[@]}"; do
    run_benchmark "${directory_path}/data_${scale_factor}" "$database_type" "$directory_path" $python_script_path
    echo ""
  done
done

# Save the current working directory
original_dir=$(pwd)

# Change the working directory to a new location
cd $python_script_path
cd "scripts"
results_path="$python_script_path/core/benchmarks/tpch/results/csv/"
python3 plotResults.py --csv_path $results_path --benchmark tpch

# Change back to the original directory
cd "$original_dir"

# Perform operations in the original directory
echo "Back to: $(pwd)"

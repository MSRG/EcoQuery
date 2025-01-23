#!/bin/bash


# Function to run dbgen script
run_dbgen() {
  scale=$1
  destination=$2
  run_dbgen_path=$3
  
  scale_value = "$scale"

  # Convert scale factor
  #if [ "$scale" == "100mb" ]; then
  #  echo "Creating data for SF $scale" 
 #   scale_value=0.1
  #elif [ "$scale" == "1gb" ]; then
  #  echo "Creating data for SF $scale"
  #  scale_value=1
  #elif [ "$scale" == "10gb" ]; then
  #  echo "Creating data for SF $scale"
  #  scale_value=10
  # elif [ "$scale" == "20gb" ]; then
  #  echo "Creating data for SF $scale"
  #  scale_value=20
 # else
  #  echo "Unsupported scale factor: $scale"
  #  exit 1
  #fi

  #"$dbgen" $scale_value $destination
  "${run_dbgen_path}/run_dbgen.sh" $scale_value $destination $run_dbgen_path
}

# Function to run python benchmark script
run_benchmark() {
  data_dir=$1
  database_type=$2
  db_path=$3 # Probably useless
  python_script_path=$4
  #Set of memory limits to try when the normal execution is killed due to OOM
  memory_limits=(10)
  
  # Save the current working directory
  original_dir=$(pwd)

  # Change the working directory to a new location
  cd $python_script_path

  # Perform operations in the new directory
  echo "Now in: $(pwd)"
  echo "$database_type"
  
  # Determine the current service name
  service_name="tpch.service"
  memory_limit=$(systemctl show -p MemoryLimit --value $service_name)
  # Check if the memory limit is set or is infinite
  if [ "$memory_limit" == "infinity" ]; then
  	echo "No memory limit is set for the service."
  	memory_limit_bytes=0
  else
  	# Convert the memory limit from bytes to gigabytes (GB)
  	memory_limit_gb=$(($memory_limit / (1024 * 1024 * 1024)))
  fi
  # Print the MemoryLimit
  if [ "$memory_limit" == "infinity" ]; then
  	echo "MemoryLimit for $service_name: unlimited"
  else
  	echo "MemoryLimit for $service_name: $memory_limit bytes ($memory_limit_gb GB)"
  fi
  
  # Empty the memory caches for cold start
  sudo sh -c 'sync && echo 3 > /proc/sys/vm/drop_caches'

  case $database_type in
    "duckdb")
      sudo sh -c 'sync && echo 3 > /proc/sys/vm/drop_caches'
      echo "DuckDB is running"
      python3 main.py --benchmark tpch --database duckdb --data $data_dir --db_path $data_dir
      # Store the exit status of the python script in a variable
      exit_status=$?
      echo "DuckDB was terminated"
      db_file="${data_dir}/tpch.db"
      # Check if file exists
      if [ -f "$db_file" ]; then
      	# If file exists, remove it
      	sudo rm "$db_file"
    	echo "File $db_file has been removed."
      fi
      echo $exit_status
      if [ "$exit_status" -ne 0 ]; then
      	sudo sh -c 'sync && echo 3 > /proc/sys/vm/drop_caches'
      	echo "Run DuckDB with memory limit through its own mechanism"
      	python3 main.py --benchmark tpch --database duckdb --data $data_dir --db_path $data_dir --mem_limit $memory_limit_gb
      	# Check if file exists
      	if [ -f "$db_file" ]; then
      	# If file exists, remove it
      	sudo rm "$db_file"
    	echo "File $db_file has been removed."
      	fi
      fi
      	
      #if [ "$?" -ne 0 ]; then
      #  echo "DuckDB is OOM"
      #  memory_info=$(free -m)
        # Extract the available memory value (in megabytes)
	#available_memory=$(echo "$memory_info" | awk 'NR==2{print $7}')
	# Convert megabytes to gigabytes
	#available_memory_gb=$(printf "%.0f" $(echo "scale=2; $available_memory / 1024" | bc))
	#echo "Run DuckDB with all available memory $available_memory_gb"
	#sudo sh -c 'sync && echo 3 > /proc/sys/vm/drop_caches'
	#python3 main.py --benchmark tpch --database duckdb --data $data_dir --db_path $db_path
	#python3 main.py --benchmark tpch --database duckdb --data $data_dir --db_path $db_path --mem_limit $available_memory_gb
      #  for mem_limit in "${memory_limits[@]}"; do
      #    echo "Run DuckDB with memory limit $mem_limit"
      #    sudo sh -c 'sync && echo 3 > /proc/sys/vm/drop_caches'
      #    python3 main.py --benchmark tpch --database duckdb --data $data_dir --db_path $db_path --mem_limit $mem_limit
      #  done
     # fi
     echo "DuckDB just ended"
      ;;
    "hyper")
      sudo sh -c 'sync && echo 3 > /proc/sys/vm/drop_caches'
      echo "Hyper is running"
      python3 main.py --benchmark tpch --database hyper --data $data_dir --db_path $data_dir
      db_file="${data_dir}/tpch.hyper"
      # Check if file exists
      if [ -f "$db_file" ]; then
      	# If file exists, remove it
      	sudo rm "$db_file"
    	echo "File $db_file has been removed."
      fi
      echo "Hyper just ended"
      ;;
    "monetdb")
      sudo sh -c 'sync && echo 3 > /proc/sys/vm/drop_caches'
      check_and_create_monetdb_files_dir "$db_path"
      echo "MonetDB is running"
      python3 main.py --benchmark tpch --database monetdb --data $data_dir --dbname tpch --dbfarm "${db_path}/monetdb_files"
      echo "MonetDB just ended"
      ;;
    "starrocks")
     #Define the path for Starrocks files !!!!!!!!!!!!!!
      starrocks_path="/home/michalis/Documents/UofT/MSRG/Carbon/starrocks_files/StarRocks-3.1.4"
      cd $starrocks_path
       #Start Starrocks server
      ./fe/bin/start_fe.sh --daemon
      sleep 1
      ./be/bin/start_be.sh --daemon
      sleep 3
      cd $python_script_path
      sudo sh -c 'sync && echo 3 > /proc/sys/vm/drop_caches'
      echo "Starrocks is running"
      python3 main.py --benchmark tpch --database starrocks --ip 127.0.0.1 --data $data_dir --name tpch
      cd $starrocks_path
      #Stop Starrocks server
      ./be/bin/stop_be.sh --daemon
      ./fe/bin/stop_fe.sh --daemon
      sleep 1
      echo "Starrocks just ended"
      # Delete data from database outside python script
      sudo rm -rf "$starrocks_path/be/storage/data"/*
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
venv_path="/home/michalis/Documents/UofT/MSRG/Carbon/codebase/olap_benchmark/venv/bin/activate"

# Activate the virtual environment
source "$venv_path"

# Check if the correct number of arguments is provided
if [ "$#" -ne 4 ]; then
  echo "Usage: $0 <directory_path> <run_dbgen_script> <python_script> <data_generation_flag>" 
  exit 1
fi

directory_path=$1 # Path where the data are located
run_dbgen_path=$2 # Path where the dbgen is located
python_script_path=$3 # Path where the python script is licated
data_gen_flag=$4 # Flag to generate data

# Define an array of scale factors
#scale_factors=("100mb" "1gb" "10gb" "20gb")

# Convert the environment variable string that contains the scale factors into an array
IFS=',' read -r -a scale_factors <<< "$SCALE_FACTORS"

# Define an array of database types
#databases=("duckdb" "hyper" "monetdb" "starrocks")
databases=("duckdb" "hyper" )

#Enable sudo access for python script
sudo chmod -R a+r /sys/class/powercap/intel-rapl

#sudo echo -1000 > /proc/$$/oom_score_adj

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
    run_benchmark "${directory_path}/data_${scale_factor}gb" "$database_type" "$directory_path" $python_script_path
    echo ""
  done
done

# Save the current working directory
original_dir=$(pwd)

# Change the working directory to a new location
cd $python_script_path
cd "scripts"
results_path="$python_script_path/core/benchmarks/tpch/results/csv/"
#python3 plotResults.py --csv_path $results_path --benchmark tpch

# Change back to the original directory
cd "$original_dir"

# Perform operations in the original directory
echo "Back to: $(pwd)"

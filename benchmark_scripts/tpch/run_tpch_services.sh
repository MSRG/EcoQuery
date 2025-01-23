#!/bin/bash

# Configuration
# -------------
read -s -p "Enter sudo password: " SUDO_PASS
echo
export SUDO_PASS

# Check required arguments
if [ "$#" -ne 4 ]; then
  echo "Usage: $0 <directory_path> <run_dbgen_script> <python_script> <data_generation_flag>" 
  exit 1
fi

# Input parameters
DIRECTORY_PATH=$1     # Path where the data are located
DBGEN_PATH=$2        # Path where the dbgen is located
PYTHON_SCRIPT_PATH=$3 # Path where the python script is located
DATA_GEN_FLAG=$4     # Flag to generate data
VENV_PATH="/media/mount/Carbon/data/tpch/venv/bin/activate"
SCALE_FACTORS=("300")
DATABASES=("starrocks" "duckdb" "hyper" "monetdb" "clickhouse")

# Helper Functions
# ---------------
run_dbgen() {
    local scale=$1
    local destination=$2
    local run_dbgen_path=$3
    "${run_dbgen_path}/run_dbgen.sh" "$scale" "$destination" "$run_dbgen_path"
}

check_and_create_dir() {
    local dir_path=$1
    if [ ! -d "$dir_path" ]; then
        echo "Creating directory: $dir_path"
        mkdir "$dir_path"
    fi
}

setup_environment() {
    source "$VENV_PATH"
    echo $SUDO_PASS | sudo -S chmod -R a+r /sys/class/powercap/intel-rapl
    echo $SUDO_PASS | sudo -S swapoff -a
    echo $SUDO_PASS | sudo -S sh -c "echo 0 > /proc/sys/vm/nr_hugepages"
}

setup_systemd_env() {
    local scale_factor=$1
    local env_file="/etc/systemd/system/tpch_benchmark.env"
    
    echo $SUDO_PASS | sudo -S bash -c "cat > $env_file << EOF
PYTHON_ENV=/media/mount/Carbon/data/tpch/venv
DATA_DIR=${DIRECTORY_PATH}/data_${scale_factor}gb
PYTHON_PATH=${PYTHON_SCRIPT_PATH}/main.py
PROJECT_PATH=${PYTHON_SCRIPT_PATH}
EOF"
}

# Database Management Functions
# ---------------------------
manage_duckdb() {
    local service_name="tpch_duckdb_mem_limit.service"
    handle_database_service "$service_name"
    cleanup_db_file "${1}/tpch.db"
}

manage_hyper() {
    local service_name="tpch_hyper.service"
    handle_database_service "$service_name"
    cleanup_db_file "${1}/tpch.hyper"
}

manage_monetdb() {
    local service_name="tpch_monetdb.service"
    check_and_create_dir "${1}/monetdb_files"
    handle_database_service "$service_name"
}

manage_starrocks() {
    local starrocks_path="/media/mount/Carbon/data/starrocks_files/StarRocks-3.1.4"
    cd "$starrocks_path"
    
    ./fe/bin/start_fe.sh --daemon
    sleep 1
    ./be/bin/start_be.sh --daemon
    sleep 50
    
    cd "$PYTHON_SCRIPT_PATH"
    handle_database_service "tpch_starrocks.service"
    
    cd "$starrocks_path"
    ./be/bin/stop_be.sh --daemon
    ./fe/bin/stop_fe.sh --daemon
    sleep 1
    
    echo $SUDO_PASS | sudo -S rm -rf "$starrocks_path/be/storage/data"/*
    cd "$PYTHON_SCRIPT_PATH"
}

manage_clickhouse() {
    sudo service clickhouse-server start
    sleep 5
    echo $SUDO_PASS | sudo -S sh -c 'sync && echo 3 > /proc/sys/vm/drop_caches'
    python3 main.py --benchmark tpch --database clickhouse --data "${1}"
    sudo service clickhouse-server stop
    sleep 3
}

# Utility Functions
# ----------------
handle_database_service() {
    local service_name=$1
    echo $SUDO_PASS | sudo -S sh -c 'sync && echo 3 > /proc/sys/vm/drop_caches'
    echo $SUDO_PASS | sudo -S systemctl daemon-reload
    echo $SUDO_PASS | sudo -S systemctl start "$service_name"
    
    while systemctl is-active --quiet "$service_name"; do
        sleep 5
    done
    
    if systemctl is-failed --quiet "$service_name"; then
        echo "$service_name failed."
    else
        echo "$service_name completed successfully."
    fi
}

cleanup_db_file() {
    local db_file=$1
    if [ -f "$db_file" ]; then
        sudo rm "$db_file"
        echo "File $db_file has been removed."
    fi
}

run_benchmark() {
    local data_dir=$1
    local database_type=$2
    local original_dir=$(pwd)
    
    cd "$PYTHON_SCRIPT_PATH"
    echo "Now in: $(pwd)"
    echo "Running $database_type"
    
    case $database_type in
        "duckdb")   manage_duckdb "$data_dir" ;;
        "hyper")    manage_hyper "$data_dir" ;;
        "monetdb")  manage_monetdb "$data_dir" ;;
        "starrocks") manage_starrocks "$data_dir" ;;
        "clickhouse") manage_clickhouse "$data_dir" ;;
        *) echo "Unsupported database type: $database_type"; exit 1 ;;
    esac
    
    cd "$original_dir"
    echo "Back to: $(pwd)"
}

# Main Script
# ----------
setup_environment

# Generate data if flag is set
if [ "$DATA_GEN_FLAG" == "true" ]; then
    for scale_factor in "${SCALE_FACTORS[@]}"; do
        data_dir="${DIRECTORY_PATH}/data_${scale_factor}"
        check_and_create_dir "$data_dir"
        run_dbgen "$scale_factor" "${DIRECTORY_PATH}/data_${scale}" "$DBGEN_PATH"
    done
fi

# Run benchmarks
for database_type in "${DATABASES[@]}"; do
    for scale_factor in "${SCALE_FACTORS[@]}"; do
        setup_systemd_env "$scale_factor"
        run_benchmark "${DIRECTORY_PATH}/data_${scale_factor}gb" "$database_type" "$DIRECTORY_PATH" "$PYTHON_SCRIPT_PATH"
        echo ""
    done
done

# Cleanup
echo $SUDO_PASS | sudo -S swapon -a
cd "$PYTHON_SCRIPT_PATH/scripts"
echo $SUDO_PASS | sudo -S sh -c "echo 0 > /proc/sys/vm/nr_hugepages"
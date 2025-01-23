#!/bin/bash

# Check if the correct number of arguments is provided
if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <scale_factor> <destination_path> <dbgen_path>"
    exit 1
fi

# Assign arguments to variables
scale_factor="$1"
destination_path="$2"
dbgen_path="$3"

# Save the current working directory
original_dir=$(pwd)

# Change the working directory to a new location
cd $dbgen_path

# Perform operations in the new directory
echo "Now in: $(pwd)"

# Run the dbgen command
#./dbgen -s "$scale_factor"
./dbgen -s "$scale_factor"

#Check if the corresponding data folder exists
if [ ! -d "$destination_path" ]; then
	mkdir -p "$destination_path"
fi

# Move the generated CSV files to the specified destination path
mv *.csv "$destination_path"

echo "CSV files generated and moved to $destination_path"

# Change back to the original directory
cd "$original_dir"

# Perform operations in the original directory
echo "Back to: $(pwd)"


#!/bin/bash

# Set the variables
QUERY_TEMPLATE_DIR="../query_templates"
OUTPUT_DIR="/home/michalis/Documents/UofT/MSRG/Carbon/benchmarks/tpcds/queries"
DIALECT="monetdb"
SCALE=10


# Loop through templates
for ((i = 1; i <= 99; i++)); do
    # Construct the template name
    TEMPLATE="query${i}.tpl"
    
    # Construct the output file name
    OUTPUT_FILE="tpcds_q${i}.sql"

    # Run the dsqgen command
    ./dsqgen -DIRECTORY "${QUERY_TEMPLATE_DIR}" -TEMPLATE "${TEMPLATE}" -VERBOSE Y -QUALIFY Y -SCALE "${SCALE}" -DIALECT "${DIALECT}" -OUTPUT_DIR "${OUTPUT_DIR}" -STREAMS 3 -RNGSEED 2
    mv "${OUTPUT_DIR}/query_0.sql" "${OUTPUT_DIR}/${OUTPUT_FILE}"

    echo "Generated: ${OUTPUT_FILE}"
done

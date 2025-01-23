import os

input_directory = ("/home/michalis/Documents/UofT/MSRG/Carbon/codebase/olap_benchmark/core/benchmarks/tpch/queries"
                   "/hyper")
output_directory = ("/home/michalis/Documents/UofT/MSRG/Carbon/codebase/olap_benchmark/core/benchmarks/tpch/queries"
                    "/hyper")

if not os.path.exists(output_directory):
    os.makedirs(output_directory)

for file_name in os.listdir(input_directory):
    if file_name.endswith('.sql'):
        with open(os.path.join(input_directory, file_name), 'r') as f:
            lines = f.readlines()

        with open(os.path.join(output_directory, file_name), 'w') as f:
            for line in lines:
                if not line.strip().startswith('--'):
                    f.write(line)
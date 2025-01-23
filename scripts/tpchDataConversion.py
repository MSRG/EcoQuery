

tables = [
            "customer",
            "lineitem",
            "nation",
            "orders",
            "part",
            "partsupp",
            "region",
            "supplier"
        ]

if __name__ == '__main__':
    data_path = "/home/michalis/Documents/UofT/MSRG/Carbon/benchmarks/tpch/data/data_100mb/"
    for table in tables:
        with open(data_path + table + ".tbl", 'r') as input_data, open(data_path + table + "_changed.tbl", 'w') as output_data:
            for line in input_data:
                modified_line = line.strip().rstrip('|') + '\n'
                output_data.write(modified_line)


tables = [
            "customer",
            "lineorder",
            "part",
            "date",
            "supplier"
        ]

if __name__ == '__main__':
    data_path = "/home/michalis/Documents/UofT/MSRG/Carbon/benchmarks/ssb/data/data_1GB/"
    for table in tables:
        with open(data_path + table + ".tbl", 'r') as input_data, open(data_path + table + ".csv", 'w') as output_data:
            for line in input_data:
                modified_line = line.strip().rstrip('|') + '\n'
                output_data.write(modified_line)
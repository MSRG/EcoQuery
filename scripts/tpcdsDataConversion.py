

tables = [
            "call_center",
            "catalog_returns",
            "customer_address",
            "customer_demographics",
            "household_demographics",
            "inventory",
            "promotion",
            "ship_mode",
            "store_returns",
            "time_dim",
            "web_page",
            "web_sales",
            "catalog_page",
            "catalog_sales",
            "customer",
            "date_dim",
            "income_band",
            "item",
            "reason",
            "store",
            "store_sales",
            "warehouse",
            "web_returns",
            "web_site"
        ]

if __name__ == '__main__':
    data_path = "/home/michalis/Documents/UofT/MSRG/Carbon/benchmarks/tpcds/data/data_1gb/"
    #remove | from the end of each line
    for table in tables:
        with open(data_path + table + ".dat", 'r',errors='replace') as input_data, open(data_path + table + "_changed.dat", 'w') as output_data:
            for line in input_data:
                #modified_line = line.strip().rstrip('|') + '\n'
                modified_line = line.rstrip()[:-1] + '\n'
                output_data.write(modified_line)
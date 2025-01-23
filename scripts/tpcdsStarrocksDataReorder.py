import csv
import os


def reorder_columns(input_csv, output_csv, column_order=None):
    with open(input_csv, 'r') as infile, open(output_csv, 'w', newline='') as outfile:
        reader = csv.reader(infile, delimiter='|')
        writer = csv.writer(outfile, delimiter='|')

        if column_order:
            for row in reader:
                reordered_row = []
                # Rearrange columns based on the specified order
                for column in column_order:
                    reordered_row.append(row[column])
                #reordered_row = [row[column_order.index(column)] for column in column_order]
                writer.writerow(reordered_row)
        else:
            # If no column reordering is specified, copy the content as-is
            for row in reader:
                writer.writerow(row)


# Example usage:

# Directory containing CSV files
csv_directory = '/home/michalis/Documents/UofT/MSRG/Carbon/benchmarks/tpcds/data/data_1gb/'

# Specify column order for files where reordering is needed
reordering_mapping = {
    'store_returns_changed.csv': [2, 9, 0, 1, 3, 4, 5, 6, 7, 8, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19],
    'catalog_returns_changed.csv': [2, 16, 0, 1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26],
    'web_returns_changed.csv': [2, 13, 0, 1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23],
    'web_sales_changed.csv': [3, 17, 0, 1, 2, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33],
    'catalog_sales_changed': [15, 17, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33],
    'store_sales_changed': [2, 9, 0, 1, 3, 4, 5, 6, 7, 8, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22],
}


# Iterate over CSV files in the directory
for filename in os.listdir(csv_directory):
    if filename.endswith('_changed.csv'):
        input_file = os.path.join(csv_directory, filename)

        # Check if column reordering is specified for this file
        column_order = reordering_mapping.get(filename)

        output_file = os.path.join(csv_directory+'starrocks_data/', f"modified_{filename}")

        # Call the function to reorder or copy columns
        reorder_columns(input_file, output_file, column_order)

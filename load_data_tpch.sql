COPY customer FROM '/home/michalis/Documents/UofT/MSRG/CloudDB discussion/TPCH/TPC-H V3.0.1/dbgen/data_100mb/customer.tbl' WITH DELIMITER '|';
COPY lineitem FROM '/home/michalis/Documents/UofT/MSRG/CloudDB discussion/TPCH/TPC-H V3.0.1/dbgen/data_100mb/lineitem.tbl' WITH DELIMITER '|';
COPY nation FROM '/home/michalis/Documents/UofT/MSRG/CloudDB discussion/TPCH/TPC-H V3.0.1/dbgen/data_100mb/nation.tbl' WITH DELIMITER '|';
COPY orders FROM '/home/michalis/Documents/UofT/MSRG/CloudDB discussion/TPCH/TPC-H V3.0.1/dbgen/data_100mb/orders.tbl' WITH DELIMITER '|';
COPY part FROM '/home/michalis/Documents/UofT/MSRG/CloudDB discussion/TPCH/TPC-H V3.0.1/dbgen/data_100mb/part.tbl' WITH DELIMITER '|';
COPY partsupp FROM '/home/michalis/Documents/UofT/MSRG/CloudDB discussion/TPCH/TPC-H V3.0.1/dbgen/data_100mb/partsupp.tbl' WITH DELIMITER '|';
COPY region FROM '/home/michalis/Documents/UofT/MSRG/CloudDB discussion/TPCH/TPC-H V3.0.1/dbgen/data_100mb/region.tbl' WITH DELIMITER '|';
COPY supplier FROM '/home/michalis/Documents/UofT/MSRG/CloudDB discussion/TPCH/TPC-H V3.0.1/dbgen/data_100mb/supplier.tbl' WITH DELIMITER '|';

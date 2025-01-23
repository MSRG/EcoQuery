import os
import subprocess
import sys
import uuid

import duckdb
from duckdb.duckdb import DuckDBPyRelation

from core.databases.db import Database


class DuckDBDatabase:

    def __init__(self, dbname, db_path, mem_limit):

        self.connection = duckdb.connect(database=db_path + "/" + dbname + ".db")
        #self.connection = duckdb.connect()
        self.db_path = db_path
        self.dbname = dbname
        self.connection.execute(f"SET temp_directory = '{db_path}/{dbname}';")

        if mem_limit is not None:
            print("Memory limit is: " + mem_limit + "GB';")
            self.connection.execute("PRAGMA memory_limit='" + mem_limit + "GB';")
            self.type = "duckdb_" + mem_limit + "GB"  # Distinguish with the limitless version of duckdb
        else:
            self.type = "duckdb"

    def getType(self):
        if "_" in self.type:
            return self.type.split("_")[0]
        else:
            return self.type

    def generate_load_queries(self, data_path, tables, ftype):
        sql_queries = []
        for table in tables:
            # If the table is 'lineitem', handle the special case for multiple files
            if table == 'lineitem':
                # List all files matching the pattern for the 'lineitem' table
                lineitem_files = [f for f in os.listdir(data_path) if
                                  f.startswith(f"{table}_part_") and f.endswith(f".{ftype}")]
                # Generate a load query for each file
                for file in lineitem_files:
                    file_path = os.path.join(data_path, file)
                    query = f"COPY {table} FROM '{file_path}' WITH DELIMITER '|';"
                    sql_queries.append(query)
            else:
                query = f"COPY {table} FROM '{data_path}/{table}.{ftype}' WITH DELIMITER '|';"
                sql_queries.append(query)

        return sql_queries, "sql"

    def create_schema(self, schema):
        duckdb.sql(schema)

    def create_schema_TPCH(self):
        duckdb.sql(
            "CREATE TABLE customer(c_custkey INTEGER NOT NULL, c_name VARCHAR(25) NOT NULL, c_address VARCHAR(40) NOT NULL, "
            "c_nationkey INTEGER NOT NULL, c_phone CHAR(15) NOT NULL, c_acctbal DECIMAL(15,2) NOT NULL, c_mktsegment CHAR(10) NOT NULL, "
            "c_comment VARCHAR(117) NOT NULL);")
        duckdb.sql(
            "CREATE TABLE lineitem(l_orderkey INTEGER NOT NULL, l_partkey INTEGER NOT NULL, l_suppkey INTEGER NOT NULL, "
            "l_linenumber INTEGER NOT NULL, l_quantity DECIMAL(15,2) NOT NULL, l_extendedprice DECIMAL(15,2) NOT NULL, "
            "l_discount DECIMAL(15,2) NOT NULL, l_tax DECIMAL(15,2) NOT NULL, l_returnflag CHAR(1) NOT NULL, l_linestatus CHAR(1) NOT NULL, "
            "l_shipdate DATE NOT NULL, l_commitdate DATE NOT NULL, l_receiptdate DATE NOT NULL, l_shipinstruct CHAR(25) "
            "NOT NULL, "
            "l_shipmode CHAR(10) NOT NULL, l_comment VARCHAR(44) NOT NULL);")
        duckdb.sql(
            "CREATE TABLE nation(n_nationkey INTEGER NOT NULL, n_name CHAR(25) NOT NULL, n_regionkey INTEGER NOT NULL,"
            " n_comment VARCHAR(152) NOT NULL);")
        duckdb.sql(
            "CREATE TABLE orders(o_orderkey INTEGER NOT NULL, o_custkey INTEGER NOT NULL, o_orderstatus CHAR(1) NOT NULL, "
            "o_totalprice DECIMAL(15,2) NOT NULL, o_orderdate DATE NOT NULL, o_orderpriority CHAR(15) NOT NULL, "
            "o_clerk CHAR(15) NOT NULL, o_shippriority INTEGER NOT NULL, o_comment VARCHAR(79) NOT NULL);")
        duckdb.sql(
            "CREATE TABLE part(p_partkey INTEGER NOT NULL, p_name VARCHAR(55) NOT NULL, p_mfgr CHAR(25) NOT NULL, "
            "p_brand CHAR(10) NOT NULL, p_type VARCHAR(25) NOT NULL, p_size INTEGER NOT NULL, p_container CHAR(10) NOT NULL, "
            "p_retailprice DECIMAL(15,2) NOT NULL, p_comment VARCHAR(23) NOT NULL);")
        duckdb.sql(
            "CREATE TABLE partsupp(ps_partkey INTEGER NOT NULL, ps_suppkey INTEGER NOT NULL, ps_availqty INTEGER NOT NULL,"
            " ps_supplycost DECIMAL(15,2) NOT NULL, ps_comment VARCHAR(199) NOT NULL);")
        duckdb.sql(
            "CREATE TABLE region(r_regionkey INTEGER NOT NULL, r_name CHAR(25) NOT NULL, r_comment VARCHAR(152) NOT NULL);")
        duckdb.sql(
            "CREATE TABLE supplier(s_suppkey INTEGER NOT NULL, s_name CHAR(25) NOT NULL, s_address VARCHAR(40) NOT NULL, "
            "s_nationkey INTEGER NOT NULL, s_phone CHAR(15) NOT NULL, s_acctbal DECIMAL(15,2) NOT NULL, "
            "s_comment VARCHAR(101) NOT NULL);")

    def load_data(self, path):
        duckdb.sql(
            "COPY customer FROM '/home/michalis/Documents/UofT/MSRG/CloudDB discussion/TPCH/TPC-H V3.0.1/dbgen/data_100mb/customer.tbl' WITH "
            "DELIMITER '|';")
        duckdb.sql(
            "COPY lineitem FROM '/home/michalis/Documents/UofT/MSRG/CloudDB discussion/TPCH/TPC-H V3.0.1/dbgen/data_100mb/lineitem.tbl' ( DELIMITER "
            "'|' );")
        duckdb.sql(
            "COPY nation FROM '/home/michalis/Documents/UofT/MSRG/CloudDB discussion/TPCH/TPC-H V3.0.1/dbgen/data_100mb/nation.tbl' WITH DELIMITER '|';")
        duckdb.sql(
            "COPY orders FROM '/home/michalis/Documents/UofT/MSRG/CloudDB discussion/TPCH/TPC-H V3.0.1/dbgen/data_100mb/orders.tbl' ( DELIMITER '|' );")
        duckdb.sql(
            "COPY part FROM '/home/michalis/Documents/UofT/MSRG/CloudDB discussion/TPCH/TPC-H V3.0.1/dbgen/data_100mb/part.tbl' WITH DELIMITER '|';")
        duckdb.sql(
            "COPY partsupp FROM '/home/michalis/Documents/UofT/MSRG/CloudDB discussion/TPCH/TPC-H V3.0.1/dbgen/data_100mb/partsupp.tbl' WITH DELIMITER '|';")
        duckdb.sql(
            "COPY region FROM '/home/michalis/Documents/UofT/MSRG/CloudDB discussion/TPCH/TPC-H V3.0.1/dbgen/data_100mb/region.tbl' WITH DELIMITER '|';")
        duckdb.sql(
            "COPY supplier FROM '/home/michalis/Documents/UofT/MSRG/CloudDB discussion/TPCH/TPC-H V3.0.1/dbgen/data_100mb/supplier.tbl' WITH DELIMITER '|';")

    def execute(self, query, print_results):
        # result = duckdb.sql(query)
        try:
            #print(f"Running query is: {query}")
            # Store query plans
            if ('COPY' or 'DROP') not in query:
                plan = self.connection.sql("EXPLAIN " + query)
                #print(plan.fetchall()[0][1])
            result = self.connection.sql(query)
            return result
        except duckdb.TransactionException as e:
            # Handle the exception here
            print(f"Caught TransactionException: {e}")
            #self.close()
            #self.destroy()
            return -1
        except duckdb.OutOfMemoryException as e:
            # Handle the exception here
            print(f"Caught TransactionException: {e}")
            #self.close()
            #self.destroy()
            return -1
        except duckdb.IOException as e:
            print(f"DuckDB out of memory: {e}")
            #self.close()
            #self.destroy()
            return -1
        except Exception as e:
            print(f"DuckDB out of memory: {e}")
            #self.close()
            #self.destroy()
            return -1

    def count_tuples(self, result: DuckDBPyRelation):
        if result is None:
            return 0
        else:
            return len(result.fetchall())

    def close(self):
        self.connection.close()

    def destroy(self):
        # Construct the full path to the database file
        db_file_path = os.path.join(self.db_path, f"{self.dbname}.db")
        
        # Check if the file exists
        if os.path.exists(db_file_path):
            # File exists, proceed to delete it
            destroy_db_cmd = f"rm {db_file_path}"
            subprocess.run(destroy_db_cmd, shell=True)
            print(f"Database {self.dbname}.db has been deleted.")
        else:
            # File does not exist
            print(f"Database {self.dbname}.db does not exist, nothing to delete.")

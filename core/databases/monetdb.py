import os.path

import pymonetdb
import subprocess

from pymonetdb.sql.cursors import Cursor


class MonetDBDatabase:

    def __init__(self, username, password, port, hostname, dbname, dbfarm):
        self.type = "monetdb"
        self.dbfarm = dbfarm
        self.dbname = dbname
        if os.path.exists(dbfarm):
            if not os.path.isfile(dbfarm + "/merovingian.log"):
                create_dbfarm_cmd = "monetdbd create " + dbfarm
                subprocess.run(create_dbfarm_cmd, shell=True)
        # if not os.path.exists(dbfarm):
        #   create_dbfarm_cmd = "monetdbd create " + dbfarm
        #    subprocess.run(create_dbfarm_cmd, shell=True)
        start_dbfarm_cmd = "monetdbd start " + dbfarm
        subprocess.run(start_dbfarm_cmd, shell=True)
        create_db_cmd = "monetdb create " + dbname
        subprocess.run(create_db_cmd, shell=True)
        start_db_cmd = "monetdb start " + dbname
        subprocess.run(start_db_cmd, shell=True)
        self.connection = pymonetdb.connect(username=username, password=password, port=port, hostname=hostname,
                                            database=dbname)
        # self.cursor = connection.cursor()

    def getType(self):
        if "_" in self.type:
            return self.type.split("_")[0]
        else:
            return self.type

    def generate_load_queries(self, data_path, tables, ftype):
        sql_queries = []
        for table in tables:
            query = f"COPY INTO {table} FROM '{data_path}/{table}.{ftype}' USING DELIMITERS '|' NULL AS '';"
            sql_queries.append(query)
        return sql_queries, "sql"

    def execute(self, query, print_results):
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            #if print_results:
            #   self.print_results(cursor)
            cursor.close()
            return 0
            #return cursor
        except Exception as e:
            print(f"Caught an exception: {e}")
            return -1

    def count_tuples(self, cursor: Cursor):
        if cursor is None:
            return 0
        else:
            return len(cursor.fetchall())

    def print_results(self, cursor):
        if cursor.rowcount <= 0 or cursor.lastrowid == -1:
            print('The resultset is empty')
        else:
            # Fetch all rows of the resultset
            rows = cursor.fetchall()
            # Print the rows
            # for row in rows:
            # print(row)

    def close(self):
        self.connection.close()

    def destroy(self):
        # Remove data from the database
        stop_db_cmd = "monetdb stop " + self.dbname
        subprocess.run(stop_db_cmd, shell=True)
        destroy_db_cmd = "monetdb destroy -f " + self.dbname
        subprocess.run(destroy_db_cmd, shell=True)
        stop_dbfarm_cmd = "monetdbd stop " + self.dbfarm
        subprocess.run(stop_dbfarm_cmd, shell=True)

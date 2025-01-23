from mysql.connector.cursor import MySQLCursor
from sqlalchemy import create_engine
from sqlalchemy.schema import Table, MetaData
from sqlalchemy.sql.expression import select, text
import pymysql
import mysql.connector
from core.benchmarks.config import columns_config


class StarrocksDatabase:

    def __init__(self, ip, dbname):
        self.type = "starrocks"
        try:
            self.connection = mysql.connector.connect(host=ip, user='root', port='9030')
            self.ip = ip
            self.dbname = dbname
            if self.connection.is_connected():
                db_Info = self.connection.get_server_info()
                print("Connected to Starrocks ", db_Info)
                createdb_query = "CREATE DATABASE IF NOT EXISTS " + dbname + ";"
                self.execute(createdb_query, True)
                usedb_query = "USE " + dbname + ";"
                self.execute(usedb_query, True)
                set_timeout = "SET GLOBAL query_timeout = 7200;"
                self.execute(set_timeout,True)
                set_query_limit = "SET GLOBAL query_mem_limit = 0;"
                self.execute(set_query_limit,True)

        except mysql.connector.Error as e:
            print("Error while connecting to Starrocks", e)

    def getType(self):
        if "_" in self.type:
            return self.type.split("_")[0]
        else:
            return self.type

    def generate_load_queries(self, data_path, tables, ftype):
        columns = columns_config.columns[self.dbname]

        commands = []
        for table in tables:
            # tableName = table + "_changed"
            tableName = table
            column_order = """ -H "columns:%s" """ % ",".join(columns[table])
            command = (
                        "curl --location-trusted -u root: -T '" + data_path + "/" + tableName + "." + ftype + "'"
                        " -H 'timeout:36000'"
                        " -H 'format: CSV'" 
                        " -H 'column_separator:|'"
                        " -H 'Expect: 100-continue'"
                        + column_order +
                        "-XPUT 'http://" + self.ip + ":8030/api/" + self.dbname + "/" + table + "/_stream_load'")
            commands.append(command)
        return commands, "shell"

    def execute(self, query, print_results):
        try:
            cursor = self.connection.cursor(buffered=True)
            for statement in filter(None, map(str.strip, query.split(';'))):
                cursor.execute(statement)
             #   if print_results:
             #       self.print_results(cursor)
             #   else:
             #       cursor.nextset()
            cursor.close()
            return 0
            #return cursor
        except Exception as e:
            print(f"Caught an exception: {e}")
            return -1

    def count_tuples(self, cursor: MySQLCursor):
        if cursor is None:
            return 0
        else:
            num = 0
            while cursor.nextset():
                rows = cursor.fetchmany(100000)
                num = num + len(rows)
            return num


    def print_results(self, cursor):
        # Fetch the results
        records = cursor.fetchall()
        for row in records:
            print(row)

    def close(self):
        self.connection.close()

    def destroy(self):
        pass

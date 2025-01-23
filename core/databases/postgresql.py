import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2 import sql

class PostgreSQLDatabase:

    def __init__(self, username, password, port, hostname, dbname):
        self.type = "psql"
        self.connection = psycopg2.connect(host=hostname, database='postgres',user=username, password=password,port = port)

        try:# Set isolation level to autocommit to create a new database
            self.connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            # Create a new cursor
            cursor = self.connection.cursor()
            # Check if the database already exists
            cursor.execute(
                sql.SQL("SELECT 1 FROM pg_database WHERE datname = %s;"),
                [dbname]
            )
            if not cursor.fetchone():
                # Create the database if it doesn't exist
                cursor.execute(
                    sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname))
                )
                print(f"Database '{dbname}' created successfully.")
            else:
                print(f"Database '{dbname}' already exists.")

            # Create a new schema with the same name as the database
            cursor.execute("CREATE SCHEMA IF NOT EXISTS " + dbname)
            # Commit the changes and close the connection
            self.connection.commit()
            # Switch to the new database
            cursor.execute("SET search_path TO "+dbname)
            cursor.execute("CREATE EXTENSION IF NOT EXISTS columnar;")
            cursor.execute("ALTER DATABASE " + dbname + " SET default_table_access_method = 'columnar';")
        except Exception as e:
            print(f"Error: {e}")

        finally:
            # Close the cursor and connection
            if cursor:
                cursor.close()


    def getType(self):
        return self.type

    def load_data(self, data_path, tables, ftype):
        cursor = self.connection.cursor()
        for table in tables:
            query = f"COPY {table} FROM STDIN DELIMITER '|' NULL AS ''"
            file = f'{data_path}/{table}_changed.{ftype}'
            # Copy data from the CSV file to the table
            with open(file, 'r') as f:
                cursor.copy_expert(query, f)
        self.connection.commit()
        cursor.close()

    def generate_load_queries(self, data_path, tables, ftype):
        sql_queries = []
        #Custom data loading
        #for table in tables:
        #    query = f"COPY {table} STDIN DELIMITER ',' CSV HEADER"
        #    file = f'{data_path}/{table}.{ftype}'
            #query = f"COPY {table} FROM '{data_path}/{table}.{ftype}' USING DELIMITERS '|' NULL AS '';"
        #    sql_queries.append((file,query))
        return sql_queries, "custom"

    def execute(self, query):
        cursor = self.connection.cursor()
        cursor.execute(query)
        self.print_results(cursor)

    def close(self):
        self.connection.close()

    def print_results(self, cursor):
        if cursor.rowcount <= 0 or cursor.lastrowid == -1:
            print('The resultset is empty')
        else:
            # Fetch all rows of the resultset
            rows = cursor.fetchall()
            # Print the rows
            for row in rows:
                print(row)




import os
import subprocess
from pathlib import Path

import clickhouse_connect
from clickhouse_connect.driver.tools import insert_file


class ClickHouseDatabase:

    def __init__(self,dbname,mem_limit,username,password,hostname):
        self.type = "clickhouse"
        connection_info = {}

        try:
            self.client = clickhouse_connect.get_client(host=hostname, username=username, password=password, send_receive_timeout = 60000)
            self.dbname = dbname
            connection_info['host'] = hostname
            connection_info['port'] = 9000
            connection_info['username'] = username
            connection_info['password'] = password

            createdb_query = "CREATE DATABASE IF NOT EXISTS " + dbname + ";"
            self.execute(createdb_query, False)
            usedb_query = "USE " + dbname + ";"
            self.execute(usedb_query, False)
            if mem_limit is not None:
                print("Memory limit is: " + mem_limit + "GB';")
                self.execute("SET max_memory_usage='" + mem_limit + "GB';", False)
                self.type = "clickhouse_" + mem_limit + "GB"

        except Exception as e:
            print(f"Failed to connect to ClickHouse: {e}")


    def getType(self):
        if "_" in self.type:
            return self.type.split("_")[0]
        else:
            return self.type

    def generate_load_queries(self, data_path, tables, ftype):
        sql_queries = []
        for table in tables:
            query = f"INSERT INTO {table} FORMAT CSV <{data_path}/{table}.{ftype}"
            sql_queries.append(query)

        return sql_queries, "sql"

    def execute(self,query,print_results):
        # Split into individual queries and filter out empty ones
        queries = [q.strip() for q in query.split(';') if q.strip()]
        #results = []
        for query in queries:
            try:
                if "INSERT INTO" in query and "<" in query:
                    if len(query.split('<')) == 2:
                        file_path = query.split('<')[1].replace(" ","")
                        table_name = Path(file_path).stem
                        insert_file(client=self.client,table=table_name,file_path=file_path,fmt='CSV',settings={"format_csv_delimiter": "|"})
                        #with open(file_path, 'r') as f:
                         #   data = f.read()
                        #result = self.client.command(query.split('<')[0],data=data,settings={"format_csv_delimiter": "|"})
                        #print(f"Data loaded successfully!: {query}...")
                        #results.append(result)
                    else:
                        print("The load for the query "+query+" is not the proper one")
                else:
                    print(f"Executing query:{query}")
                    result = self.client.query(query)
                    print(f"Successfully executed: {query}...")
                    #results.append(result)

            except Exception as e:
                print(f"Caught an exception: {e}")
                return -1

        return 0

    def close(self):
        self.client.close()

    def destroy(self):
        pass

    def insert_csv_to_clickhouse(csv_path, table_name, host='localhost', port=9000, user=None, password=None):
        """
        Inserts a CSV file into ClickHouse using the command-line client.

        This approach is more reliable for large files because:
        1. It uses ClickHouse's native client, which handles streaming efficiently
        2. It avoids potential memory issues that can occur with Python drivers
        3. It inherits all the optimizations built into the ClickHouse client

        Args:
            csv_path: Path to the CSV file
            table_name: Name of the target table
            host: ClickHouse server hostname (default: localhost)
            port: ClickHouse server port (default: 9000)
            user: ClickHouse username (optional)
            password: ClickHouse password (optional)
        """
        # Verify the CSV file exists
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV file not found: {csv_path}")

        # Build the ClickHouse client command
        clickhouse_cmd = ['clickhouse-client']

        # Add connection parameters if provided
        if host:
            clickhouse_cmd.extend(['--host', host])
        if port:
            clickhouse_cmd.extend(['--port', str(port)])
        if user:
            clickhouse_cmd.extend(['--user', user])
        if password:
            clickhouse_cmd.extend(['--password', password])

        # Construct the INSERT query using INFILE
        query = f"INSERT INTO {table_name} FORMAT CSV INFILE '{csv_path}'"
        clickhouse_cmd.extend(['--query', query])

        try:
            # Execute the command and capture output
            result = subprocess.run(
                clickhouse_cmd,
                check=True,  # This will raise an exception if the command fails
                text=True,
                capture_output=True  # Capture stdout and stderr
            )
            print("Data insertion completed successfully")
            return True

        except subprocess.CalledProcessError as e:
            # Handle command execution errors
            print(f"Error executing ClickHouse client command:")
            print(f"Exit code: {e.returncode}")
            print(f"Error output: {e.stderr}")
            raise

        except Exception as e:
            print(f"Unexpected error: {str(e)}")
            raise

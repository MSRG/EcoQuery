import subprocess

from tableauhyperapi import HyperProcess, Telemetry, Connection, CreateMode, Result


class HyperDatabase:

    def __init__(self, dbname, db_path, mem_limit):
        self.type = "hyper"
        self.db_path = db_path
        self.dbname = dbname
        if mem_limit is not None:
            process_parameters = {"memory_limit": str(mem_limit * 1000) + "m"}
            self.hyper = HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU,
                                      parameters=process_parameters)
            self.connection = Connection(endpoint=self.hyper.endpoint, database=db_path + "/" + dbname + ".hyper",
                                         create_mode=CreateMode.CREATE_IF_NOT_EXISTS)
            self.connection.catalog.create_database_if_not_exists(dbname)
            self.connection.catalog.create_schema_if_not_exists(dbname)
        else:
            self.hyper = HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU)
            self.connection = Connection(endpoint=self.hyper.endpoint, database=db_path + "/" + dbname + ".hyper",
                                         create_mode=CreateMode.CREATE_IF_NOT_EXISTS)
            self.connection.catalog.create_database_if_not_exists(dbname)
            self.connection.catalog.create_schema_if_not_exists(dbname)

    def getType(self):
        if "_" in self.type:
            return self.type.split("_")[0]
        else:
            return self.type

    def generate_load_queries(self, data_path, tables, ftype):
        sql_queries = []
        # ftype = 'csv'
        for table in tables:
            query = f"COPY {table} FROM '{data_path}/{table}.{ftype}' WITH ( FORMAT => 'csv', DELIMITER => '|' , header => false);"
            sql_queries.append(query)

        return sql_queries, "sql"

    def execute(self, query, print_results):
        try:
            for statement in filter(None, map(str.strip, query.split(';'))):
                results = self.connection.execute_query(statement)
                results.close()
            return None  #Return None because Hyper requires to close every pending result
        except Exception as e:
            print(f"Caught an exception: {e}")
            return -1

    def count_tuples(self, result):
        if result is None:
            return 0
        else:
            return result

    def close(self):
        self.connection.close()
        self.hyper.close()

    def destroy(self):
        destroy_db_cmd = f"rm {self.db_path}/{self.dbname}.hyper"
        subprocess.run(destroy_db_cmd, shell=True)

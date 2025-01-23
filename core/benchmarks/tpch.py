import os
import sys

import time
import psutil
from core.benchmarks.benchmark import Benchmark
from core.tracker import Tracker
import utils
import subprocess


class TPCHBenchmark(Benchmark):

    def __init__(self, print_results, clear_cache, interval):

        self.tables = [
            "customer",
            "lineitem",
            "nation",
            "orders",
            "part",
            "partsupp",
            "region",
            "supplier"
        ]
        self.path = "core/benchmarks/tpch/"
        self.type = "tpch"
        # Scale factor
        self.sf = None
        self.print_results = print_results
        self.clear_cache = clear_cache
        self.interval = interval

    def createSchema(self, db):
        print("Creating TPCH schema in " + db.getType())
        with open('core/benchmarks/tpch/' + db.getType() + '_schema.sql', 'r') as file:
            schema = file.read()
        results = db.execute(schema, self.print_results)

    def loadData(self, db, data_path):
        load_tuple = db.generate_load_queries(data_path, self.tables, "csv")
        tracker = Tracker(self.interval, "tpch", "tpch-load data")
        io_counters_start = psutil.disk_io_counters(perdisk=True)
        tracker.start()
        result = 0  # Check if there is any exceptions during query execution
        start_time = time.time()
        try:
            if load_tuple[1] == "sql":
                for query in load_tuple[0]:
                    result = db.execute(query, self.print_results)
            elif load_tuple[1] == "shell":
                for command in load_tuple[0]:
                    subprocess.run(command, shell=True, check=True)
            elif load_tuple[1] == "custom":
                db.load_data(data_path, self.tables, "tbl")
        except MemoryError:
            # Handle MemoryError gracefully
            print("MemoryError: Out of memory occurred.")
            # Perform cleanup actions if necessary
            sys.exit(1)
        end_time = time.time()
        tracker.stop()
        if result == -1:
            sys.exit(-1)
        io_counters_end = psutil.disk_io_counters(perdisk=True)
        elapsed_time = end_time - start_time

        # Calculate and export all measured metrics
        utils.calculate_run_stats(self, db.type, "load data", tracker.results, io_counters_start, io_counters_end,
                                  elapsed_time)

        # Clear memory cache
        clear_cache_cmd = ['sh', '-c', '"echo 3 | tee /proc/sys/vm/drop_caches"']
        command_with_sudo = f"echo 18911891 | sudo -S {' '.join(clear_cache_cmd)}"
        if self.clear_cache:
            subprocess.run(['sync'], check=True)
            subprocess.run(command_with_sudo, shell=True)

        # Get the current process ID
        #process = psutil.Process()

        # Get the memory usage in bytes
        #memory = process.memory_info().rss

        # Convert the memory usage to megabytes
        #memory_mb = memory / (1024 * 1024)

        #print(f"The current memory usage is {memory_mb:.2f} MB.")

    def run(self, db):
        queries = []
        qpath = self.path + "queries/" + db.getType()
        for j in range(1, 23):
            queryName = "tpch-q" + str(j) + ".sql"
            with open(os.path.join(qpath, queryName), "r") as file:
                query = file.read()
                queries.append(query)
        total_stream_time = 0
        counter = 1
        clear_cache_cmd = ['sh', '-c', '"echo 3 | tee /proc/sys/vm/drop_caches"']
        command_with_sudo = f"echo 18911891 | sudo -S {' '.join(clear_cache_cmd)}"
        current_working_directory = os.getcwd()
        # Check if the directory exists
        if not os.path.exists(f"{current_working_directory}/core/benchmarks/tpch/results/duckdb_plans/"):
            # Create the directory
            os.makedirs(f"{current_working_directory}/core/benchmarks/tpch/results/duckdb_plans/")
        for query in queries:
            if self.clear_cache:
                subprocess.run(['sync'], check=True)
                subprocess.run(command_with_sudo, shell=True)
            tracker = Tracker(self.interval, "tpch", "tpch-q" + str(counter))
            io_counters_start = psutil.disk_io_counters(perdisk=True)
            tracker.start()
            start_time = time.time()
            result = db.execute(query, self.print_results)
            end_time = time.time()
            tracker.stop()
            io_counters_end = psutil.disk_io_counters(perdisk=True)
            elapsed_time = end_time - start_time
            if result == -1:
                utils.calculate_run_stats(self, db.type, "tpch-q" + str(counter), tracker.results, io_counters_start,
                                          io_counters_end,
                                          0) # 0 when the query is not finishing
                counter = counter + 1
                continue
                #sys.exit(-1)
            utils.calculate_run_stats(self, db.type, "tpch-q" + str(counter), tracker.results, io_counters_start,
                                      io_counters_end,
                                      elapsed_time)
            total_stream_time = total_stream_time + elapsed_time
            counter = counter + 1

        # Number of queries in the stream
        num_queries = len(queries)
        # Calculate and print throughput
        throughput = num_queries / total_stream_time
        utils.export_throughput(self, db.type, throughput)

        if db.type == 'hyper':
            db.close()

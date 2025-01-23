import os
import sys

import psutil
import time
from core.benchmarks.benchmark import Benchmark
from core.tracker import Tracker
import utils
import subprocess


class SSBBenchmark(Benchmark):

    def __init__(self, print_results, clear_cache, interval):
        # with open('core/benchmarks/ssb/ssb-schema.sql', 'r') as file:
        #    self.schema = file.read()
        self.tables = [
            "customer",
            "lineorder",
            "part",
            "date",
            "supplier"
        ]
        self.path = "core/benchmarks/ssb/"
        self.type = "ssb"
        self.print_results = print_results
        self.clear_cache = clear_cache
        self.interval = interval

    def createSchema(self, db):
        print("Creating SSB schema in " + db.getType())
        with open('core/benchmarks/ssb/' + db.getType() + '_schema.sql', 'r') as file:
            schema = file.read()
        db.execute(schema, self.print_results)

    def loadData(self, db, data_path):
        print("Loading SSB data in " + db.getType())
        load_tuple = db.generate_load_queries(data_path, self.tables, "csv")
        tracker = Tracker(self.interval, "ssb", "ssb-load data")
        io_counters_start = psutil.disk_io_counters(perdisk=True)
        tracker.start()
        exit_code = 0
        start_time = time.time()
        if load_tuple[1] == "sql":
            for query in load_tuple[0]:
                exit_code = db.execute(query, self.print_results)
        elif load_tuple[1] == "shell":
            for command in load_tuple[0]:
                subprocess.run(command, shell=True)
        elif load_tuple[1] == "custom":
            db.load_data(data_path, self.tables, "tbl")
        end_time = time.time()
        tracker.stop()
        if exit_code == -1:
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

    def run(self, db):
        queries = []
        qpath = self.path + "queries/" + db.getType()
        for j in range(1, 14):
            queryName = "ssb-q" + str(j) + ".sql"
            with open(os.path.join(qpath, queryName), "r") as file:
                query = file.read()
                queries.append(query)
        total_stream_time = 0
        counter = 0
        clear_cache_cmd = ['sh', '-c', '"echo 3 | tee /proc/sys/vm/drop_caches"']
        command_with_sudo = f"echo 18911891 | sudo -S {' '.join(clear_cache_cmd)}"
        for query in queries:
            if self.clear_cache:
                subprocess.run(['sync'], check=True)
                subprocess.run(command_with_sudo, shell=True)
            tracker = Tracker(self.interval, "ssb", "ssb-q" + str(counter))
            io_counters_start = psutil.disk_io_counters(perdisk=True)
            tracker.start()
            start_time = time.time()
            exit_code = db.execute(query, self.print_results)
            end_time = time.time()
            tracker.stop()
            if exit_code == -1:
                sys.exit(-1)
            io_counters_end = psutil.disk_io_counters(perdisk=True)
            elapsed_time = end_time - start_time
            # utils.print_tracker_results("ssb-q" + str(counter), tracker.results)
            # Calculate and export all measured metrics
            utils.calculate_run_stats(self, db.type, "ssb-q" + str(counter), tracker.results, io_counters_start,
                                      io_counters_end,
                                      elapsed_time)
            # utils.export_query_stats(self, db.type, "ssb-q" + str(counter), tracker.results, elapsed_time)
            total_stream_time = total_stream_time + elapsed_time
            counter = counter + 1

        # Number of queries in the stream
        num_queries = len(queries)
        # Calculate and print throughput
        throughput = num_queries / total_stream_time
        utils.export_throughput(self, db.type, throughput)

        if db.type == 'hyper':
            db.close()

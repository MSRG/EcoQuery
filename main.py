import argparse

import utils
from core.benchmarks.benchmark_factory import BenchmarkFactory
from core.databases.dbfactory import DatabaseFactory
from core.rapl_config import rapl_reader

if __name__ == '__main__':
    # pyRAPL.setup()

    rapl_reader.setup()


    parser = argparse.ArgumentParser(description="Run five different benchmarks.")
    parser.add_argument("--benchmark", type=str, default=1, choices=["tpch", "tpcds", "ssb"],
                        help="Choose between tpch,tpcds,ssb", required=True)
    parser.add_argument("--database", type=str, default=1, choices=["duckdb", "psql", "monetdb", "starrocks", "hyper","clickhouse"],
                        help="Choose between duckdb,psql,monetdb,starrocks,hyper,clickhouse", required=True)
    parser.add_argument("--data", type=str, help="the path where benchmark data are stored", required=True)
    parser.add_argument("--print", action='store_true', help="Print the query results or not")
    parser.add_argument("--clear-cache", action='store_true', help="Clear the memory cache after each task ")
    parser.add_argument("--interval", type=float, default=2, help="Measurement interval")
    if "duckdb" in parser.parse_known_args()[0].database:
        parser.add_argument("--db_path", type=str, help="path of persistent storage")
        parser.add_argument("--mem_limit", type=str, help="memory limit of the database")
    if "hyper" in parser.parse_known_args()[0].database:
        parser.add_argument("--db_path", type=str, help="path of persistent storage")
        parser.add_argument("--mem_limit", type=str, help="memory limit of the database")
    if "monetdb" in parser.parse_known_args()[0].database:
        parser.add_argument("--username", type=str, default="monetdb", help="username of the database")
        parser.add_argument("--password", type=str, default="monetdb", help="password of the database")
        parser.add_argument("--port", type=int, default=50000, help="port of the database")
        parser.add_argument("--hostname", type=str, default="localhost", help="hostname of the database")
        parser.add_argument("--dbname", type=str, help="name of the database")
        parser.add_argument("--dbfarm", type=str, help="dbfarm of the database")
    if "clickhouse" in parser.parse_known_args()[0].database:
        parser.add_argument("--username", type=str, default="default", help="username of the database")
        parser.add_argument("--password", type=str, default="123", help="password of the database")
        parser.add_argument("--port", type=int, default=9000, help="port of the database")
        parser.add_argument("--hostname", type=str, default="localhost", help="hostname of the database")
        parser.add_argument("--mem_limit", type=str, help="memory limit of the database")
    if "starrocks" in parser.parse_known_args()[0].database:
        parser.add_argument("--ip", type=str, help="ip address of starrocks' fe")
        parser.add_argument("--name", type=str, help="name of the database")
    if "psql" in parser.parse_known_args()[0].database:
        parser.add_argument("--username", type=str, default="postgres", help="username of the database")
        parser.add_argument("--password", type=str, default="postgres", help="password of the database")
        parser.add_argument("--port", type=int, default=5432, help="port of the database")
        parser.add_argument("--hostname", type=str, default="localhost", help="hostname of the database")
        parser.add_argument("--dbname", type=str, help="name of the database")
    parser.add_argument("--iterations", type=int, default=1000, help="Number of iterations for each benchmark.")
    args = parser.parse_args()
    # print(args.benchmark)

    # Prepare for benchmark execution
    benchmark_factory = BenchmarkFactory()
    benchmark = benchmark_factory.create_benchmark(args.benchmark, args.print, args.clear_cache, args.interval)
    # Set the scale factor for the current execution of the benchmark
    benchmark.sf = utils.extract_sf_from_path(args.data)
    db_factory = DatabaseFactory()
    db = db_factory.create_database(args)
    print("Execute benchmark " + args.benchmark + " on database " + db.type + " with SF " + benchmark.sf)
    utils.initializeOutput(benchmark, db.type)
    benchmark.createSchema(db)

    # Calculate the energy consumption in idle state
    utils.calculate_idle_average_energy(16, benchmark, db.type)
    print("Before load data")
    benchmark.loadData(db, args.data)
    # calculate_average_energy(16)
    benchmark.run(db)
    db.close()
    db.destroy()
    print("Benchmark " + args.benchmark + " on database " + db.type + " with SF " + benchmark.sf + " is complete")

from core.databases.clickhouse import ClickHouseDatabase
from core.databases.duckdb import DuckDBDatabase
from core.databases.hyper import HyperDatabase
from core.databases.postgresql import PostgreSQLDatabase
from core.databases.monetdb import MonetDBDatabase
from core.databases.starrocks import StarrocksDatabase


class DatabaseFactory:
    def create_database(self, args):
        db_type = args.database
        if db_type == 'duckdb':
            return DuckDBDatabase(args.benchmark, args.db_path, args.mem_limit)
        elif db_type == 'psql':
            return PostgreSQLDatabase(args.username, args.password, args.port, args.hostname, args.dbname)
        elif db_type == 'monetdb':
            return MonetDBDatabase(args.username, args.password, args.port, args.hostname, args.dbname, args.dbfarm)
        elif db_type == "starrocks":
            return StarrocksDatabase(args.ip, args.name)
        elif db_type == "hyper":
            return HyperDatabase(args.benchmark, args.db_path, args.mem_limit)
        elif db_type == "clickhouse":
            return ClickHouseDatabase(args.benchmark,args.mem_limit,args.username, args.password,args.hostname)

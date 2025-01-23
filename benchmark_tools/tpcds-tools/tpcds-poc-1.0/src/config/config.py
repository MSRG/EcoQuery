#!/usr/bin/env python
# -- coding: utf-8 --
import os

BENCHMARKS = ["ssb", "tpch", "tpcds"]
# the default benchmark to test
BENCHMARK = "tpcds"

PROJECT_ROOT_PATH = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "../.."))
# StarRocks's conf file
STARROCKS_CONF = os.path.join(PROJECT_ROOT_PATH, "conf/starrocks.conf")
# the root directory of all SQL
SQL_ROOT = os.path.join(PROJECT_ROOT_PATH, "sql")
# the result directory of all result
RESULT_ROOT = os.path.join(PROJECT_ROOT_PATH, "result")

################################################################################
# load config info
TPCDS_CONCURRENCY_LOAD_CONFIG = {
    "catalog_sales": 10,
    "store_sales": 10
}

# concurrency for big table to load in parallel
CONCURRENCY_LOAD_CONFIG = {
    "tpcds": TPCDS_CONCURRENCY_LOAD_CONFIG
}

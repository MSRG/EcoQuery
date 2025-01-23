#!/bin/bash

sudo cp tpch_duckdb.service /etc/systemd/system
sudo cp tpch_duckdb_mem_limit.service /etc/systemd/system
sudo cp tpch_hyper.service /etc/systemd/system
sudo cp tpch_monetdb.service /etc/systemd/system
sudo cp tpch_starrocks.service /etc/systemd/system
sudo cp tpch_clickhouse.service /etc/systemd/system

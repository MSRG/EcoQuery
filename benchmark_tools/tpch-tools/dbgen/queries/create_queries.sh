#!/bin/bash

for ((i=1;i<=22;i++)); do
  ./qgen -v -c -s 0.1 ${i} > tpch-q${i}.sql
done

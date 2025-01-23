#!/bin/bash

for((i=1;i<=10;i++))
do
    ./dsdgen -scale 10 -PARALLEL 10 -CHILD $i -TERMINATE N
done

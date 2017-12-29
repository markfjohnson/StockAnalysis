#!/usr/bin/env bash

dcos spark run --docker-image="markfjohnson/spark_pandas" --submit-args="--conf spark.executor.memory=4g --total-executor-cores 10 https://raw.githubusercontent.com/markfjohnson/StockAnalysis/master/sec_indexes.py" --verbose
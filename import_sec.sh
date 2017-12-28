#!/usr/bin/env bash

dcos spark run --docker-image="markfjohnson/spark_pandas" --submit-args="https://raw.githubusercontent.com/markfjohnson/StockAnalysis/master/sec_indexes.py" --verbose
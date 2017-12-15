#!/usr/bin/env bash

dcos spark run --docker-image="markfjohnson/spark_pandas" --submit-args="https://raw.githubusercontent.com/markfjohnson/StockAnalysis/a839db761d86b663ba5b44fbd9d6f1a52ffa29d5/scan_sec_filing_topic.py" --verbose
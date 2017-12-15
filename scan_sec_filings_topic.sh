#!/usr/bin/env bash
#!/usr/bin/env bash

dcos spark run --docker-image="markfjohnson/spark_pandas" --submit-args="https://raw.githubusercontent.com/markfjohnson/StockAnalysis/26e9dc1ef9f7ef69c04d862fab590aefb0a6df19/scan_sec_filing_topic.py" --verbose
# Spark-TPC-DS
Spark job for the TPC-DS benchmark.

This code uses this library from Databricks: https://github.com/databricks/spark-sql-perf

To compile put the jar compiled from the above library in lib/ and then run `build/sbt package`

To execute the following arguments must be provided:

1. HDFS data location ("/user/test/tpcds-data")
2. scale factor (10)
3. HDFS result location ("/user/test/tpcds-results")


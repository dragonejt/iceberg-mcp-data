import sys
from pyspark.sql import functions as F
from databricks.sdk.runtime import spark

catalog = sys.argv[1]
schema = sys.argv[2]

file_downloads = spark.read.table("bigquery.pypi.file_downloads")

table = f"{catalog}.{schema}.pypi_file_downloads"

if spark.catalog.tableExists(table):
    file_downloads.writeTo(table).append()
else:
    file_downloads.writeTo(table).using("delta").create()

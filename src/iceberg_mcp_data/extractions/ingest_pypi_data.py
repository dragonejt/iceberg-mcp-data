from datetime import datetime, timezone, timedelta
from pyspark.sql import functions as F
from databricks.sdk.runtime import spark, dbutils

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

previous_day = (datetime.now(timezone.utc) - timedelta(days=1)).date()

file_downloads = spark.read.table("bigquery.pypi.file_downloads")
file_downloads = file_downloads.filter(
    (F.col("timestamp") >= F.lit(previous_day)) & (F.col("timestamp") < F.lit(previous_day + timedelta(days=1)))
)

table = f"{catalog}.{schema}.pypi_file_downloads"

if spark.catalog.tableExists(table):
    file_downloads.writeTo(table).append()
else:
    file_downloads.writeTo(table).using("delta").create()

from datetime import datetime, timezone, timedelta
from databricks.sdk.runtime import spark

previous_day = (datetime.now(timezone.utc) - timedelta(days=1)).date()

file_downloads = spark.read.table("bigquery.pypi.file_downloads")

file_downloads.write.format("delta").mode("append").save("pypi_file_downloads")

from databricks.sdk.runtime import spark
from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


@dp.table
@dp.expect("download_count >= 0", F.col("download_count") >= 0)
def download_counts() -> DataFrame:
    df = spark.read.table("workspace.default.file_downloads")
    df = df.withColumn("date", F.to_date("timestamp"))
    df = df.withColumn("version", F.col("file.version"))
    df = df.groupBy("date", "version")
    df = df.agg(F.count("*").alias("download_count"))
    df = df.orderBy("date", "version")

    return df

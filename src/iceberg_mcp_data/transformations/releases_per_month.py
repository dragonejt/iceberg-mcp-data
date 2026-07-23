from databricks.sdk.runtime import spark
from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


@dp.table
def releases_per_month() -> DataFrame:
    df = spark.read.table("workspace.default.distribution_metadata")
    df = df.groupBy(F.year("upload_time").alias("year"), F.month("upload_time").alias("month"))
    df = df.agg(F.count("*").alias("releases"))
    df = df.orderBy("year", "month")

    return df

from pyspark.sql.connect import functions as F
from pyspark.sql.connect.dataframe import DataFrame
from pyspark.sql.connect.session import SparkSession

from iceberg_mcp_data.config import PipelineConfig


def download_counts(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    df = spark.read.table("DEFAULT.PUBLIC.FILE_DOWNLOADS")
    df = df.withColumn("date", F.to_date("timestamp"))
    df = df.withColumn("version", F.col("file.version"))
    df = df.groupBy("date", "version")
    df = df.agg(F.count("*").alias("download_count"))
    df = df.orderBy("date", "version")

    if config.debug is True:
        df.show(10)

    return df

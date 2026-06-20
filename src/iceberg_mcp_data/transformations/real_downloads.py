from pyspark.sql.connect import functions as F
from pyspark.sql.connect.dataframe import DataFrame
from pyspark.sql.connect.session import SparkSession

from iceberg_mcp_data.transformations import table


@table
def real_downloads(spark: SparkSession) -> DataFrame:
    df = spark.read.table("DEFAULT.PUBLIC.FILE_DOWNLOADS")
    df = df.filter(F.col("details.installer.name").isin("pip", "uv"))
    df = df.dropDuplicates()

    return df

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from databricks.sdk.runtime import spark


@dp.table
def real_downloads():
    df = spark.read.table("workspace.default.pypi_file_downloads")
    df = df.filter(F.col("details.installer.name").isin("pip", "uv"))
    df = df.dropDuplicates()

    return df

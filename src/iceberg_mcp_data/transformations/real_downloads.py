from databricks.sdk.runtime import spark
from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


@dp.materialized_view(
    table_properties={
        "delta.columnMapping.mode": "name",
        "delta.enableRowTracking": "true",
        "delta.enableIcebergCompatV3": "true",
        "delta.universalFormat.enabledFormats": "iceberg",
    }
)
def real_downloads() -> DataFrame:
    df = spark.read.table("workspace.default.file_downloads")
    df = df.filter(F.col("details.installer.name").isin("pip", "uv"))
    df = df.dropDuplicates()

    return df

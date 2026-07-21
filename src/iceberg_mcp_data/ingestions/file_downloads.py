from base64 import b64encode

from databricks.sdk.runtime import dbutils, spark
from pyspark.sql import SparkSession

credentials = dbutils.secrets.get(scope="gcp", key="bigquery")
credentials = b64encode(credentials.encode()).decode()


def file_downloads(spark: SparkSession) -> None:
    query = """
    SELECT *
    FROM `bigquery-public-data.pypi.file_downloads`
    WHERE project = "iceberg-mcp-server"
    AND DATE(timestamp) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    """

    file_downloads = (
        spark.read.format("bigquery")
        .option("credentials", credentials)
        .option("query", query)
        .option("viewsEnabled", "true")
        .option("parentProject", "dragonejt")
        .option("materializationProject", "dragonejt")
        .option("materializationDataset", "databricks")
        .load()
    )

    table = "workspace.default.file_downloads"

    if spark.catalog.tableExists(table):
        file_downloads.writeTo(table).append()
    else:
        (
            file_downloads.writeTo(table)
            .using("iceberg")
            .tableProperty("format-version", "3")
            .tableProperty("write.delete.mode", "merge-on-read")
            .create()
        )


if __name__ == "__main__":
    file_downloads(spark)

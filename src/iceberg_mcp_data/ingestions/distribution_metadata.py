from base64 import b64encode
from datetime import datetime, timezone

from databricks.sdk.runtime import dbutils, spark
from pyspark.sql import SparkSession


def distribution_metadata(spark: SparkSession, credentials: str) -> None:
    # Only executes on first of each month to get previous month's data
    if datetime.now(timezone.utc).day != 1:
        return

    query = """
    SELECT *
    FROM `bigquery-public-data.pypi.distribution_metadata`
    WHERE name = 'iceberg-mcp-server'
    AND DATE_TRUNC(DATE(upload_time), MONTH) = DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH)
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

    table = "workspace.default.distribution_metadata"

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
    credentials = dbutils.secrets.get(scope="gcp", key="bigquery")
    credentials = b64encode(credentials.encode()).decode()

    distribution_metadata(spark, credentials)

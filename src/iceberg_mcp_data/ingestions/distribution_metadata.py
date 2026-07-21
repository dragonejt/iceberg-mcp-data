from base64 import b64encode

from databricks.sdk.runtime import dbutils, spark

credentials = dbutils.secrets.get(scope="gcp", key="bigquery")
credentials = b64encode(credentials.encode()).decode()

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

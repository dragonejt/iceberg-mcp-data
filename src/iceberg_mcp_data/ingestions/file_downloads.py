from base64 import b64encode
from databricks.sdk.runtime import spark, dbutils

credentials = dbutils.secrets.get(scope="gcp", key="bigquery")
credentials = b64encode(credentials.encode()).decode()

query = """
SELECT *
FROM `bigquery-public-data.pypi.file_downloads`
WHERE project = "iceberg-mcp-server"
AND DATE(timestamp) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
"""

file_downloads = (
    spark.read.format("bigquery")
    .option("credentials", credentials)
    .option(
        "query",
        "SELECT * FROM `bigquery-public-data.pypi.file_downloads` WHERE project = 'iceberg-mcp-server' AND DATE(timestamp) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)",
    )
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
        .tableProperty("delta.columnMapping.mode", "id")
        .tableProperty("delta.enableIcebergCompatV2", "true")
        .tableProperty("delta.universalFormat.enabledFormats", "iceberg")
        .create()
    )

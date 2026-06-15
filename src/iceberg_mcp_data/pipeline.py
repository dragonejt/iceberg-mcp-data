from enum import Enum

from cyclopts import App
from snowflake.snowpark_connect import init_spark_session

from iceberg_mcp_data.config import PipelineConfig
from iceberg_mcp_data.transformations.download_counts import download_counts

app = App()


class JobName(Enum):
    download_counts = "download_counts"


@app.default
def run_pipeline(config: PipelineConfig = PipelineConfig()):
    spark = init_spark_session()
    spark.conf.set("snowpark.connect.iceberg.external_volume", "SNOWFLAKE_MANAGED")

    download_counts(spark, config).writeTo("DOWNLOAD_COUNTS").using("iceberg").createOrReplace()

    spark.stop()


@app.command
def run_job(job: JobName, config: PipelineConfig = PipelineConfig()):
    spark = init_spark_session()
    spark.conf.set("snowpark.connect.iceberg.external_volume", "SNOWFLAKE_MANAGED")

    match job:
        case JobName.download_counts:
            download_counts(spark, config).writeTo("DOWNLOAD_COUNTS").using("iceberg").createOrReplace()

    spark.stop()


if __name__ == "__main__":
    app()

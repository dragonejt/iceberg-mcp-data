from collections.abc import Generator
from contextlib import contextmanager

from cyclopts import App
from pyspark.sql.connect.session import SparkSession
from snowflake.snowpark_connect import init_spark_session

from iceberg_mcp_data.config import PipelineConfig
from iceberg_mcp_data.transformations.download_counts import download_counts
from iceberg_mcp_data.transformations.real_downloads import real_downloads


class IcebergMCPDataPipeline:
    app: App

    def __init__(self) -> None:
        self.app = App()
        self.app.default(self.run_pipeline)
        self.app.command(self.download_counts)
        self.app.command(self.real_downloads)

    @contextmanager
    def spark_session(self) -> Generator[SparkSession]:
        spark = init_spark_session()
        spark.conf.set(
            "snowpark.connect.iceberg.external_volume",
            "SNOWFLAKE_MANAGED",
        )
        try:
            yield spark
        finally:
            spark.stop()

    def run_pipeline(self, config: PipelineConfig = PipelineConfig()) -> None:
        """Iceberg MCP Data Pipeline"""

        with self.spark_session() as spark:
            download_counts(spark, config)
            real_downloads(spark, config)

    def download_counts(self, config: PipelineConfig = PipelineConfig()) -> None:
        """Run the download_counts transformation."""

        with self.spark_session() as spark:
            download_counts(spark, config)

    def real_downloads(self, config: PipelineConfig = PipelineConfig()) -> None:
        """Run the real_downloads transformation."""

        with self.spark_session() as spark:
            real_downloads(spark, config)


pipeline = IcebergMCPDataPipeline().app


if __name__ == "__main__":
    pipeline()

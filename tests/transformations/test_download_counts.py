from pyspark.sql.connect.session import SparkSession

from iceberg_mcp_data.config import PipelineConfig
from iceberg_mcp_data.transformations.download_counts import download_counts
from tests.spark_test_case import SparkTestCase


class TestDownloadCounts(SparkTestCase):
    spark: SparkSession

    def setUp(self) -> None:
        self.config = PipelineConfig(False)

    def test_successful_download_counts(self) -> None:
        df = download_counts(self.spark, self.config)

        self.assertEqual(set(df.columns), set(["date", "version", "download_count"]))

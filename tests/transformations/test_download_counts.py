from pyspark.sql.connect.session import SparkSession

from iceberg_mcp_data.config import PipelineConfig
from iceberg_mcp_data.transformations.download_counts import download_counts
from tests.spark_test_case import SparkTestCase


class TestDownloadCounts(SparkTestCase):
    spark: SparkSession

    def setUp(self):
        self.config = PipelineConfig(False)

    def test_download_counts(self):
        df = download_counts(self.spark, self.config)

        self.assertEqual(set(df.columns), set(["date", "version", "download_count"]))

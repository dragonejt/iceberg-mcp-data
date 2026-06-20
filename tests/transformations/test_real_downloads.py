from pyspark.sql.connect.session import SparkSession

from iceberg_mcp_data.config import PipelineConfig
from iceberg_mcp_data.transformations.real_downloads import real_downloads
from tests.spark_test_case import SparkTestCase


class TestRealDownloads(SparkTestCase):
    spark: SparkSession

    def setUp(self):
        self.config = PipelineConfig(debug=True)

    def test_successful_real_downloads(self) -> None:
        df = real_downloads(self.spark, self.config)

        self.assertGreaterEqual(df.count(), df.distinct().count())
        self.assertEqual(df.select("details.installer.name").distinct().count(), 2)

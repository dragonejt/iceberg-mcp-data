from unittest import TestCase

from pyspark.sql.connect.session import SparkSession
from snowflake.snowpark_connect import init_spark_session


class SparkTestCase(TestCase):
    spark: SparkSession | None = None

    @classmethod
    def setUpClass(cls) -> None:
        if SparkTestCase.spark is None:
            SparkTestCase.spark = init_spark_session()

        cls.spark = SparkTestCase.spark

    @classmethod
    def teardownClass(cls) -> None:
        pass

    @classmethod
    def tearDownModule(cls) -> None:
        if SparkTestCase.spark is not None:
            SparkTestCase.spark.stop()

from collections.abc import Callable
from functools import wraps
from typing import Protocol

from pyspark.sql.connect.dataframe import DataFrame
from pyspark.sql.connect.session import SparkSession

from iceberg_mcp_data.config import PipelineConfig


class Transform(Protocol):
    __name__: str

    def __call__(self, spark: SparkSession) -> DataFrame: ...


def table(transform: Transform, table_name: str | None = None) -> Callable[[SparkSession, PipelineConfig], DataFrame]:
    if table_name is None:
        table_name = transform.__name__.upper()

    @wraps(transform)
    def wrapper(spark: SparkSession, config: PipelineConfig) -> DataFrame:
        df = transform(spark)

        if config.debug is True:
            print(table_name)
            df.show()
        else:
            df.writeTo(table_name).using("iceberg").createOrReplace()

        return df

    return wrapper

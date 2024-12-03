from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, SparkSession
from common.debug_utils import debug_schema_mismatch
import logging

class BaseTransformer(ABC):
    def __init__(self, spark: SparkSession, schema_config: dict):
        self.spark = spark
        self.schema_config = schema_config

    @abstractmethod
    def transform(self, df: DataFrame) -> tuple[DataFrame, ...]:
        """Transform the input DataFrame into one or more output DataFrames"""
        pass

    @abstractmethod
    def get_table_names(self) -> list[str]:
        """Return list of target table names"""
        pass

    def validate_schema(self, df: DataFrame, expected_schema) -> bool:
        """Validate DataFrame schema matches expected schema"""
        if df.schema != expected_schema:
            logging.error("Schema validation failed")
            debug_schema_mismatch(df, expected_schema)
            return False
        return True
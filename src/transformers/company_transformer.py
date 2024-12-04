from models.base_table_transformer import BaseTableTransformer
from pyspark.sql import DataFrame
from typing import Tuple, List

class CompanyTransformer(BaseTableTransformer):
    def __init__(self, spark, schema_config):
        super().__init__(spark, schema_config, 'channel1_company')

    def transform(self, data) -> List[Tuple[str, DataFrame]]:
        """
        Transform the input data into DataFrames.
        This method wraps transform_docs_to_dataframes to satisfy the abstract method requirement.
        """
        return self.transform_docs_to_dataframes(data)
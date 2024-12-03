from typing import Tuple, List
from pyspark.sql import DataFrame
from models.base_table_transformer import BaseTableTransformer
from models.base_nested_transformer import BaseNestedTransformer

class OrderFabricFootprintTransformer(BaseNestedTransformer):
    def __init__(self, spark, schema_config):
        super().__init__(spark, schema_config, 'channel1_order_fabric_Footprint')

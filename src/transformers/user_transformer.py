from models.base_table_transformer import BaseTableTransformer

class UserTransformer(BaseTableTransformer):
    def __init__(self, spark, schema_config):
        super().__init__(spark, schema_config, 'channel1_user')
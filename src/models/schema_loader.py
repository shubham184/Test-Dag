import os
import yaml
from typing import Dict, Any
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType, 
    TimestampType, MapType, ArrayType, IntegerType, FloatType, DoubleType
)

class SchemaLoader:
    TYPE_MAPPING = {
        'string': StringType(),
        'boolean': BooleanType(),
        'integer': IntegerType(),
        'float': FloatType(),
        'timestamp': TimestampType(),
        'map<string,string>': MapType(StringType(), StringType()),
        'array<integer>': ArrayType(IntegerType()),
        'array<map<string,string>>': ArrayType(MapType(StringType(), StringType())),
        # Add other types as needed
    }

    @classmethod
    def parse_fields(cls, fields_config):
        schema_fields = []
        for field in fields_config:
            # Skip generated fields
            if field.get('generated'):
                continue

            field_name = field['name']
            field_type = field['type']
            nullable = field.get('nullable', True)

            if field_type == 'struct':
                # Handle nested struct
                nested_fields = cls.parse_fields(field['fields'])
                spark_type = StructType(nested_fields)
            else:
                spark_type = cls.TYPE_MAPPING.get(field_type, StringType())

            schema_fields.append(StructField(field_name, spark_type, nullable))
        return schema_fields

    @classmethod
    def create_schema_from_config(cls, config: Dict[str, Any]) -> StructType:
        fields = cls.parse_fields(config['fields'])
        return StructType(fields)

    @classmethod
    def load_schema_config(cls, schema_dir: str) -> Dict[str, Any]:
        schema_config = {}

        for filename in os.listdir(schema_dir):
            if filename.endswith(('.yml', '.yaml')):
                file_path = os.path.join(schema_dir, filename)
                with open(file_path, 'r') as file:
                    config = yaml.safe_load(file)

                    for table_name, table_config in config.items():
                        # Create spark schema
                        table_config['spark_schema'] = cls.create_schema_from_config(table_config)
                        schema_config[table_name] = table_config

        return schema_config
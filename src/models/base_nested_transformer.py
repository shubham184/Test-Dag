from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, explode, monotonically_increasing_id, concat, lit
)
from pyspark.sql.types import StringType
import logging
from typing import Tuple, Dict, List
from models.base_transformer import BaseTransformer

class BaseNestedTransformer(BaseTransformer):
    def __init__(self, spark: SparkSession, schema_config: dict, table_name: str):
        super().__init__(spark, schema_config)
        self.table_config = schema_config[table_name]

    def transform(self, docs: list) -> Tuple[DataFrame, ...]:
        """Transform raw docs to extract nested data"""
        logging.info(f"Starting {self.table_config['table_name']} transformation")
        
        nested_data = []
        fields = self.table_config['fields']
        field_mapping = {
            field['name']: field.get('source_field', field['name']) 
            for field in fields
        }

        # Get path from configuration
        path = self.table_config.get('path', [])
        source_id_field = self.table_config.get('source_id_field', 'ID')
        
        for doc in docs:
            # Navigate to nested data using configured path
            current_data = doc
            valid_data = True
            
            for path_step in path:
                field = path_step.get('field')
                if field and field in current_data:
                    current_data = current_data[field]
                else:
                    valid_data = False
                    break

            if not valid_data:
                continue

            # Handle array data
            if self.table_config.get('is_array', False):
                if isinstance(current_data, list):
                    data_items = current_data
                else:
                    continue
            else:
                data_items = [current_data] if current_data else []

            # Process each item
            for item in data_items:
                if isinstance(item, dict):
                    # Get source ID
                    source_id = item.get(source_id_field, '')
                    if not source_id:
                        continue

                    # Initialize with required fields
                    item_data = {
                        'nested_id': '',  # Will be generated
                        'id': '',         # Will be generated
                        'parent_id': source_id
                    }

                    # Extract rest of the fields
                    extracted_data = self._extract_field_values(item, field_mapping)
                    item_data.update(extracted_data)
                    nested_data.append(item_data)

        if not nested_data:
            logging.warning(f"No {self.table_config['table_name']} data found in documents")
            schema = self.table_config['spark_schema']
            return (self.spark.createDataFrame([], schema),)

        try:
            df = self.spark.createDataFrame(nested_data)
            logging.info(f"Created DataFrame with {df.count()} rows")

            # Generate IDs
            df = df.withColumn("row_id", monotonically_increasing_id())

            # Generate nested_id (primary key)
            df = df.withColumn(
                "nested_id",
                concat(
                    lit(f"{self.table_config['table_name']}_"),
                    col("parent_id"),
                    lit("_"),
                    col("row_id").cast(StringType())
                )
            )

            # Generate id
            df = df.withColumn(
                "id",
                concat(
                    lit("footprint_"),
                    col("row_id").cast(StringType())
                )
            )

            df = df.drop("row_id")

            # Select columns in order specified by schema
            select_columns = [
                field['name'] 
                for field in self.table_config['fields']
            ]

            result_df = df.select(*select_columns)

            # Log sample for verification
            logging.info("Sample of generated data:")
            result_df.select("nested_id", "id", "parent_id").show(5, truncate=False)

            logging.info(f"Final DataFrame schema: {result_df.schema}")
            logging.info(f"Number of rows in final DataFrame: {result_df.count()}")

            return (result_df,)

        except Exception as e:
            logging.error(f"Error creating DataFrame: {str(e)}")
            logging.error(f"Available columns: {df.columns if 'df' in locals() else 'DataFrame not created'}")
            logging.error(f"Required columns: {select_columns if 'select_columns' in locals() else []}")
            raise

    def _extract_field_values(self, data: Dict, field_mapping: Dict) -> Dict:
        """Extract field values based on schema configuration"""
        result = {}
        
        for field_name, source_field in field_mapping.items():
            # Skip id fields as they're handled separately
            if field_name in ['nested_id', 'id', 'parent_id']:
                continue

            value = data
            
            # Handle nested field paths
            if '.' in str(source_field):
                for key in source_field.split('.'):
                    value = value.get(key, {})
            else:
                value = data.get(source_field, '')

            # Handle different value types
            if isinstance(value, (dict, list)):
                value = str(value)

            result[field_name] = str(value) if value is not None else ''

        return result

    def get_table_names(self) -> List[str]:
        """Return the table names this transformer is responsible for"""
        return [self.table_config['table_name']]
    
    def transform_docs_to_dataframes(self, docs: list) -> List[DataFrame]:
        """Bridge method to align with BaseTableTransformer interface"""
        return list(self.transform(docs))
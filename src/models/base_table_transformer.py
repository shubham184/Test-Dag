from abc import ABC
from array import ArrayType
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import MapType, StringType, DataType
from pyspark.sql.functions import (
    col, lit, monotonically_increasing_id, 
    concat, to_timestamp, explode
)
import logging
from typing import Dict, List, Tuple
from models.base_transformer import BaseTransformer
from models.base_nested_transformer import BaseNestedTransformer
from models.schema_loader import SchemaLoader

"""
BaseTableTransformer is responsible for transforming raw documents into
DataFrames for both main and nested tables based on a given schema configuration.

Attributes:
    spark (SparkSession): The Spark session used for DataFrame operations.
    schema_config (dict): Configuration dictionary containing schema details.
    table_type (str): The type of table being transformed.
    table_config (dict): Configuration for the specific table type.
    table_configs (dict): Configuration for all tables.

Methods:
    transform_docs_to_dataframes(docs): Transforms raw documents into DataFrames
        for main and nested tables.
    _process_float_fields(docs, float_fields): Ensures float fields in documents
        are correctly typed.
    _transform_all_tables(main_df): Transforms the main table and all nested tables.
    _process_nested_tables(nested_tables, parent_df, parent_table_name, result_dfs,
        parent_table_config, docs): Processes nested tables recursively.
    _transform_nested_field(parent_df, nested_config, target_config, field_type,
        parent_table_config): Transforms a nested field based on its type.
    _get_nested_field_type(target_config): Determines the type of a nested field.
    _transform_nested_primitive(parent_df, nested_config, target_config,
        parent_table_config): Transforms nested arrays of primitives.
    _transform_nested_struct(parent_df, nested_config, target_config,
        parent_table_config): Transforms nested arrays of structs.
    _transform_single_struct(parent_df, nested_config, target_config,
        parent_table_config): Transforms nested single structs.
    _get_spark_type(field_name, target_config): Retrieves the Spark data type for
        a field from the target configuration.
    _log_nested_table_stats(df, nested_config): Logs statistics for a nested table.
    get_table_names(): Retrieves all table names including nested tables recursively.
    _collect_nested_table_names(table_config, table_names): Collects names of nested
        tables recursively.
"""

class BaseTableTransformer(BaseTransformer):
    def __init__(self, spark: SparkSession, schema_config: dict, table_type: str):
        super().__init__(spark, schema_config)
        self.table_config = schema_config[f'{table_type}']
        self.table_configs = schema_config
        self.table_type = table_type

    def transform_docs_to_dataframes(self, docs: list) -> List[Tuple[str, DataFrame]]:
        """
        Transform raw docs into main and nested table DataFrames.

        Args:
            docs (list): List of raw documents to be transformed.

        Returns:
            List[Tuple[str, DataFrame]]: List of tuples where each tuple contains the table name and the corresponding DataFrame.
        """
        # Store the docs for use in nested transformers
        self.original_docs = docs

        # Create schema from configuration
        table_schema = self.table_config['spark_schema']
        
        # Create a new list of docs with '_id' renamed to 'relationship_id'
        renamed_docs = []
        for doc in docs:
            renamed_doc = doc.copy()
            renamed_doc['relationship_id'] = renamed_doc.pop('_id', None)
            renamed_docs.append(renamed_doc)
        
        # Process float fields if they exist
        float_fields = [field['name'] for field in self.table_config['fields'] 
                        if field['type'] == 'float']
        
        if float_fields:
            self._process_float_fields(renamed_docs, float_fields)
        
        # Create main DataFrame
        try:
            df = self.spark.createDataFrame(renamed_docs, schema=table_schema)
        except Exception as e:
            logging.error(f"Error creating DataFrame: {e}")
            raise e
        
        # Apply timestamp transformations
        for field in self.table_config['fields']:
            if field['type'] == 'timestamp':
                df = df.withColumn(field['name'], to_timestamp(col(field['name'])))

        # Transform main and nested tables
        return self._transform_all_tables(df)


    def _process_float_fields(self, docs: List[Dict], float_fields: List[str]):
        """Process float fields to ensure values are floats.

        This method iterates through a list of documents and converts specified fields to float type.

        Args:
            docs (List[Dict]): List of dictionaries containing document data
            float_fields (List[str]): List of field names that should be converted to float type

        Raises:
            ValueError: If a field value cannot be converted to float

        Returns:
            None: The input docs list is modified in place

        Example:
            >>> docs = [{'price': '10.5', 'quantity': 5}]
            >>> float_fields = ['price', 'quantity']
            >>> _process_float_fields(docs, float_fields)
            >>> print(docs)
            [{'price': 10.5, 'quantity': 5.0}]
        """
        """Process float fields to ensure values are floats"""
        for doc in docs:
            for field_name in float_fields:
                if field_name in doc and doc[field_name] is not None:
                    if isinstance(doc[field_name], int):
                        doc[field_name] = float(doc[field_name])
                    elif not isinstance(doc[field_name], float):
                        try:
                            doc[field_name] = float(doc[field_name])
                        except ValueError as e:
                            logging.error(f"Cannot convert field {field_name} with value {doc[field_name]} to float.")
                            raise e

    def _transform_all_tables(self, main_df: DataFrame) -> List[Tuple[str, DataFrame]]:
        """Transform main dataframe into a list of tuples containing table names and their corresponding transformed dataframes.

        This method processes both the main table and its nested tables according to the table configuration.
        It extracts non-nested fields for the main table and recursively processes any nested tables.

        Args:
            main_df (DataFrame): The main input DataFrame to be transformed

        Returns:
            List[Tuple[str, DataFrame]]: A list of tuples where each tuple contains:
                - str: The table name
                - DataFrame: The transformed DataFrame for that table

        Example:
            result = transformer._transform_all_tables(input_df)
            # Returns: [('main_table', main_df), ('nested_table1', nested_df1), ...]
        """
        """Transform main table and all nested tables"""
        result_dfs = []
        # Process main table
        main_fields = [
            field['name']
            for field in self.table_config['fields']
            if not field.get('nested_table')
        ]
        main_table_df = main_df.select(*main_fields)
        result_dfs.append((self.table_config['table_name'], main_table_df))

        # Process nested tables
        if 'nested_tables' in self.table_config:
            nested_tables = self.table_config['nested_tables']
            # Process nested tables recursively
            self._process_nested_tables(
                nested_tables,
                parent_df=main_table_df,
                parent_table_name=self.table_config['table_name'],
                result_dfs=result_dfs,
                parent_table_config=self.table_config,
                docs=self.original_docs  # Pass the original docs
            )

        return result_dfs



    def _process_nested_tables(
        self,
        nested_tables: List[Dict],
        parent_df: DataFrame,
        parent_table_name: str,
        result_dfs: List[Tuple[str, DataFrame]],
        parent_table_config: Dict,
        docs: list
    ):
        for nested_config in nested_tables:
            logging.info(f"Processing nested table: {nested_config['target_table']}")
            target_config = self.table_configs[nested_config['target_table']]

            # Check if use_nested_transformer is True
            if nested_config.get('use_nested_transformer') or target_config.get('use_nested_transformer'):
                # Use BaseNestedTransformer to process this nested table
                nested_transformer = BaseNestedTransformer(self.spark, self.table_configs, nested_config['target_table'])
                # Use docs to transform
                nested_dfs = nested_transformer.transform_docs_to_dataframes(docs)
                # Add the result DataFrame(s) to result_dfs
                result_dfs.extend([(target_config['table_name'], df) for df in nested_dfs])
            else:
                # Proceed as before
                nested_field_type = self._get_nested_field_type(target_config)
                logging.info(f"Detected nested field type: {nested_field_type}")

                # Get the parent DataFrame
                parent_table = nested_config.get('parent_table', parent_table_name)
                parent_df = next(df for name, df in result_dfs if name == parent_table)
                # logging.info(f"Parent table: {parent_table}")
                # logging.info(f"Parent DataFrame schema: {parent_df.schema}")

                # Transform the nested field
                result_df = self._transform_nested_field(
                    parent_df,
                    nested_config,
                    target_config,
                    nested_field_type,
                    parent_table_config
                )

                # Add the result DataFrame to result_dfs
                result_dfs.append((target_config['table_name'], result_df))

            # If the nested table has further nested tables, process them recursively
            if 'nested_tables' in target_config:
                nested_parent_df = next(df for name, df in result_dfs if name == target_config['table_name'])
                self._process_nested_tables(
                    target_config['nested_tables'],
                    parent_df=nested_parent_df,
                    parent_table_name=target_config['table_name'],
                    result_dfs=result_dfs,
                    parent_table_config=target_config,
                    docs=docs
                )



    def _transform_nested_field(
        self,
        parent_df: DataFrame,
        nested_config: Dict,
        target_config: Dict,
        field_type: str,
        parent_table_config: Dict
    ) -> DataFrame:
        """Transform nested field based on its type"""
        transform_methods = {
            'primitive': self._transform_nested_primitive,
            'struct': self._transform_nested_struct,
            'single_struct': self._transform_single_struct,
            # 'array_of_maps': self._transform_array_of_maps
        }

        transform_method = transform_methods.get(field_type)
        if not transform_method:
            raise ValueError(f"Unknown nested field type: {field_type}")

        result_df = transform_method(parent_df, nested_config, target_config, parent_table_config)
        self._log_nested_table_stats(result_df, nested_config)
        count = result_df.count()
        return result_df



    def _get_nested_field_type(self, target_config: Dict) -> str:
        """Determine the type of the nested field"""
        fields = target_config['fields']
        data_fields = [
            field for field in fields
            if not field.get('generated') and field['name'] not in ['id', target_config.get('foreign_key')]
        ]
        if target_config.get('is_array', False):
            # Check if all fields are maps
            # if target_config.get('nested_field', False):
            #     return 'array_of_maps'
            if len(data_fields) == 1:
                return 'primitive'
            else:
                return 'struct'
        else:
            return 'single_struct'


    def _transform_nested_primitive(
        self,
        parent_df: DataFrame,
        nested_config: Dict,
        target_config: Dict,
        parent_table_config: Dict  # Accept parent table config
    ) -> DataFrame:
        """Transform nested array of primitives"""
        nested_df = parent_df.select(
            col(parent_table_config['primary_key']),
            explode(col(nested_config['field'])).alias(nested_config['field'])
        )

        nested_df = nested_df.withColumn("row_id", monotonically_increasing_id())

        nested_field_name = [
            field['name']
            for field in target_config['fields']
            if not field.get('generated') and field['name'] not in ['id', nested_config['foreign_key']]
        ][0]

        select_exprs = [
            concat(
                col(parent_table_config['primary_key']),
                lit("_"),
                col("row_id").cast(StringType())
            ).alias("id"),
            col(parent_table_config['primary_key']).alias(nested_config['foreign_key']),
            col(nested_config['field']).alias(nested_field_name)
        ]

        count = nested_df.count()
        return nested_df.select(*select_exprs)


    def _transform_nested_struct(
        self,
        parent_df: DataFrame,
        nested_config: Dict,
        target_config: Dict,
        parent_table_config: Dict  # Accept parent table config
    ) -> DataFrame:
        """Transform nested array of structs"""
        nested_df = parent_df.select(
            col(parent_table_config['primary_key']),
            explode(col(nested_config['field'])).alias("nested_struct")
        )

        nested_df = nested_df.filter(col("nested_struct").isNotNull())
        nested_df = nested_df.withColumn("row_id", monotonically_increasing_id())

        nested_fields = [
            field['name']
            for field in target_config['fields']
            if not field.get('generated') and field['name'] not in ['nested_id', nested_config['foreign_key']]
        ]

        select_exprs = [
            concat(
                col(parent_table_config['primary_key']),
                lit("_"),
                col("row_id").cast(StringType())
            ).alias("nested_id"),
            col(parent_table_config['primary_key']).alias(nested_config['foreign_key'])
        ]

        for field in nested_fields:
            field_expr = col("nested_struct")[field]
            field_type = self._get_spark_type(field, target_config)
            select_exprs.append(field_expr.cast(field_type).alias(field))

        count = nested_df.count()
        return nested_df.select(*select_exprs)

    def _transform_single_struct(
        self,
        parent_df: DataFrame,
        nested_config: Dict,
        target_config: Dict,
        parent_table_config: Dict
    ) -> DataFrame:
        """Transform nested single structs (like admin and contact)"""
        nested_field_name = nested_config['field']
        
        # # Log initial schema and sample data
        # logging.info(f"\n{'='*80}")
        # logging.info(f"Processing {nested_field_name} as single struct")
        # logging.info(f"Initial DataFrame Schema: {parent_df.schema}")
        # logging.info("Sample of input data:")
        # parent_df.select(nested_field_name).show(2, truncate=False)
        
        # Create select expressions
        select_exprs = [
            concat(
                col(parent_table_config['primary_key']),
                lit(f"_{nested_field_name}")
            ).alias("id"),
            col(parent_table_config['primary_key']).alias(nested_config['foreign_key'])
        ]
        
        # # Log target fields we're trying to extract
        # logging.info("\nTarget fields from config:")
        # for field in target_config['fields']:
        #     if field['name'] not in ['id', nested_config['foreign_key']]:
        #         logging.info(f"Field: {field['name']}, Type: {field['type']}")
        
        # Add expressions for each field
        for field in target_config['fields']:
            field_name = field['name']
            if field_name not in ['id', nested_config['foreign_key']]:
                field_expr = col(f"{nested_field_name}.{field_name}")
                field_type = self._get_spark_type(field_name, target_config)
                select_exprs.append(
                    field_expr.cast(field_type).alias(field_name)
                )
        
        # Create result DataFrame
        result_df = parent_df.select(*select_exprs)
        
        # # Log final result
        # logging.info("\nFinal result schema:")
        # logging.info(result_df.schema)
        # logging.info("\nSample of final results:")
        # result_df.show(2, truncate=False)
        
        # # Log value presence check for each field
        # logging.info("\nChecking for non-null values in each field:")
        # for field in result_df.schema.fields:
        #     count = result_df.filter(col(field.name).isNotNull()).count()
        #     total = result_df.count()
        #     logging.info(f"Field {field.name}: {count}/{total} non-null values")
        
        # logging.info(f"{'='*80}\n")
        
        return result_df



    # def _transform_array_of_maps(
    #     self,
    #     parent_df: DataFrame,
    #     nested_config: Dict,
    #     target_config: Dict,
    #     parent_table_config: Dict
    # ) -> DataFrame:
    #     """Transform nested array of maps"""
    #     # Debug logging
    #     logging.info(f"Input field name: {nested_config['field']}")
        
    #     # Filter for non-null values and show sample
    #     non_null_df = parent_df.filter(col(nested_config['field']).isNotNull())
    #     logging.info(f"Count of rows with non-null {nested_config['field']}: {non_null_df.count()}")
    #     logging.info("Sample of non-null data:")
    #     non_null_df.select(nested_config['field']).show(2, truncate=False)
        
    #     field_name = nested_config['field']
        
    #     # Now process only non-null values
    #     nested_df = non_null_df.select(
    #         col(parent_table_config['primary_key']),
    #         explode(col(field_name)).alias("nested_map")
    #     )
        
    #     logging.info(f"Count after explode: {nested_df.count()}")
    #     logging.info("Sample after explode:")
    #     nested_df.show(2, truncate=False)
        
    #     nested_df = nested_df.withColumn("row_id", monotonically_increasing_id())

    #     select_exprs = [
    #         concat(
    #             col(parent_table_config['primary_key']),
    #             lit("_"),
    #             col("row_id").cast(StringType())
    #         ).alias("id"),
    #         col(parent_table_config['primary_key']).alias(nested_config.get('foreign_key', 'parent_id'))
    #     ]

    #     for field in target_config['fields']:
    #         if field['name'] not in ['id', nested_config.get('foreign_key')]:
    #             field_name = field['name']
    #             field_type = self._get_spark_type(field_name, target_config)
    #             select_exprs.append(
    #                 col("nested_map")[field_name].cast(field_type).alias(field_name)
    #             )

    #     result_df = nested_df.select(*select_exprs)
        
    #     # Final count
    #     count = result_df.count()
    #     logging.info(f"Final row count: {count}")
    #     if count > 0:
    #         logging.info("Sample of final data:")
    #         result_df.show(2, truncate=False)
        
    #     return result_df

    def _get_spark_type(self, field_name: str, target_config: Dict) -> DataType:
        """Get Spark data type for a field from the target configuration"""
        field_type_str = next((f['type'] for f in target_config['fields'] 
                                if f['name'] == field_name), 'string')
        return SchemaLoader.TYPE_MAPPING.get(field_type_str, StringType())
    def _log_nested_table_stats(self, df: DataFrame, nested_config: Dict):
        """Log statistics for nested table"""
        logging.info(f"Nested table {nested_config['target_table']} statistics:")
        logging.info(f"Total rows: {df.count()}")

    def get_table_names(self) -> List[str]:
        """Get all table names including nested tables recursively"""
        table_names = [self.table_config['table_name']]
        self._collect_nested_table_names(self.table_config, table_names)
        return table_names

    def _collect_nested_table_names(self, table_config: Dict, table_names: List[str]):
        if 'nested_tables' in table_config:
            for nested in table_config['nested_tables']:
                nested_table_name = nested['target_table']
                if nested_table_name not in table_names:
                    table_names.append(nested_table_name)
                    nested_table_config = self.table_configs.get(nested_table_name, {})
                    self._collect_nested_table_names(nested_table_config, table_names)

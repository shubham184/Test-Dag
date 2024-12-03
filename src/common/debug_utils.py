import logging
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

def debug_schema_mismatch(df: DataFrame, expected_schema: StructType):
    """
    Debug helper to identify schema mismatches between DataFrame and expected schema
    """
    df_fields = set((field.name, field.dataType.typeName()) for field in df.schema.fields)
    expected_fields = set((field.name, field.dataType.typeName()) for field in expected_schema.fields)
    
    # Find missing and extra fields
    missing_fields = expected_fields - df_fields
    extra_fields = df_fields - expected_fields
    
    if missing_fields:
        logging.error(f"Missing fields in DataFrame: {missing_fields}")
    if extra_fields:
        logging.error(f"Extra fields in DataFrame: {extra_fields}")
        
    # Check type mismatches for common fields
    common_fields = df_fields.intersection(expected_fields)
    df_field_dict = {name: dtype for name, dtype in df_fields}
    expected_field_dict = {name: dtype for name, dtype in expected_fields}
    
    type_mismatches = []
    for name, _ in common_fields:
        if df_field_dict[name] != expected_field_dict[name]:
            type_mismatches.append((name, df_field_dict[name], expected_field_dict[name]))
    
    if type_mismatches:
        for name, df_type, expected_type in type_mismatches:
            logging.error(f"Type mismatch for field '{name}': DataFrame has {df_type}, expected {expected_type}")
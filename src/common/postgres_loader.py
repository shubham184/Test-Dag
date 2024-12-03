import psycopg2
import json
import logging
from typing import Dict, Any
from psycopg2 import sql
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit, to_json, create_map
from pyspark.sql.types import StringType, BooleanType, LongType, TimestampType, MapType, ArrayType

def prepare_value_for_postgres(value: Any) -> Any:
    """Prepare a value for PostgreSQL insertion"""
    if value is None:
        return None
    elif isinstance(value, (dict, list)):
        return json.dumps(value)
    elif isinstance(value, str):
        return value.replace('\x00', '').replace('\x0b', '')
    return value

def load_data_to_postgres(spark, df: DataFrame, db_params: Dict[str, Any], table_name: str) -> None:
    """
    Load DataFrame data into PostgreSQL table with improved error handling
    """
    try:
        # Get the primary key from schema
        primary_key = None
        for field in df.schema.fields:
            if isinstance(field.dataType, StringType):
                primary_key = field.name
                break
                
        if not primary_key:
            raise ValueError("No suitable primary key field found in schema")

        # Create table
        # table_name = table_name.replace('-', '_').lower()
        # table_name = table_name.lower()
        # logging.info(f"Creating table {table_name}")
        # logging.info(f"original table name: {table_name}")
        create_table_query = generate_create_table_query(df.schema, table_name, primary_key)
        
        # Execute create table
# from psycopg2 import sql

        with psycopg2.connect(**db_params) as conn:
            with conn.cursor() as cursor:
                # Replace '-' with '_'
                drop_table_query = sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(sql.Identifier(table_name))
                cursor.execute(drop_table_query)
                cursor.execute(create_table_query)
                conn.commit()



        # Handle null values
        df = handle_null_values(df)

        # Convert DataFrame to rows
        rows = df.collect()
        count = df.count()
        
        # Batch insert
        batch_size = 1000
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            insert_batch(batch, db_params, table_name, primary_key)

        # Verify count
        with psycopg2.connect(**db_params) as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                db_count = cursor.fetchone()[0]
                
        total_rows = df.count()
        logging.info(f"Loaded {db_count} of {total_rows} rows into {table_name}")
        
        if db_count != total_rows:
            logging.warning(
                f"Mismatch between DataFrame rows ({total_rows}) and "
                f"database rows ({db_count}) in {table_name}"
            )
            
    except Exception as e:
        logging.error(f"Failed to load data into {table_name}: {str(e)}")
        raise

def generate_create_table_query(schema, table_name: str, primary_key: str) -> str:
    """Generate CREATE TABLE query based on DataFrame schema"""
    columns = []
    for field in schema.fields:
        if isinstance(field.dataType, StringType):
            pg_type = "TEXT"
        elif isinstance(field.dataType, BooleanType):
            pg_type = "BOOLEAN"
        elif isinstance(field.dataType, LongType):
            pg_type = "BIGINT"
        elif isinstance(field.dataType, TimestampType):
            pg_type = "TIMESTAMP"
        elif isinstance(field.dataType, (MapType, ArrayType)):
            pg_type = "JSONB"
        else:
            pg_type = "TEXT"
        
        columns.append(f"{field.name} {pg_type}")
    
    columns.append(f"PRIMARY KEY ({primary_key})")
    
    return f"""
    CREATE TABLE {table_name} (
        {', '.join(columns)}
    )
    """

def handle_null_values(df: DataFrame) -> DataFrame:
    """Handle null values in DataFrame"""
    for field in df.schema.fields:
        if isinstance(field.dataType, MapType):
            df = df.withColumn(
                field.name,
                when(col(field.name).isNull(), to_json(create_map()))
                .otherwise(to_json(col(field.name)))
            )
        elif isinstance(field.dataType, ArrayType):
            df = df.withColumn(
                field.name,
                when(col(field.name).isNull(), to_json(lit([])))
                .otherwise(to_json(col(field.name)))
            )
        elif isinstance(field.dataType, StringType):
            df = df.withColumn(
                field.name,
                when(col(field.name).isNull() | (col(field.name) == ''), None)
                .otherwise(col(field.name))
            )
        elif isinstance(field.dataType, BooleanType):
            df = df.withColumn(
                field.name,
                when(col(field.name).isNull(), False)
                .otherwise(col(field.name))
            )
        elif isinstance(field.dataType, LongType):
            df = df.withColumn(
                field.name,
                when(col(field.name).isNull(), 0)
                .otherwise(col(field.name))
            )
    return df

def insert_batch(batch, db_params: Dict[str, Any], table_name: str, primary_key: str) -> None:
    """Insert a batch of rows into PostgreSQL"""
    with psycopg2.connect(**db_params) as conn:
        with conn.cursor() as cursor:
            for row in batch:
                row_dict = row.asDict()
                
                # Prepare column names and values
                columns = list(row_dict.keys())
                values = [prepare_value_for_postgres(v) for v in row_dict.values()]
                
                # Build the UPSERT query
                placeholders = ', '.join(['%s' for _ in values])
                update_set = ', '.join([
                    f"{col} = EXCLUDED.{col}"
                    for col in columns
                    if col != primary_key
                ])
                
                insert_query = f"""
                    INSERT INTO {table_name} ({', '.join(columns)})
                    VALUES ({placeholders})
                    ON CONFLICT ({primary_key})
                    DO UPDATE SET {update_set}
                """
                
                try:
                    cursor.execute(insert_query, values)
                except Exception as e:
                    logging.error(f"Error inserting row: {str(e)}")
                    logging.error(f"Problematic row: {row_dict}")
                    conn.rollback()
                    continue
            
            conn.commit()
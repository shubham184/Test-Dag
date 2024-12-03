from airflow import DAG
from airflow.decorators import task
from datetime import datetime
import os
import sys
import logging

# Add the src directory to Python path
dag_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.join(dag_dir, 'src')
sys.path.append(src_dir)

# Import your existing modules
from src.common.spark_session import create_spark_session
from src.common.config_loader import ConfigLoader
from src.models.schema_loader import SchemaLoader
from src.transformers.user_transformer import UserTransformer
from src.transformers.company_transformer import CompanyTransformer
from src.transformers.order_transformer import OrderTransformer
from src.transformers.orderline_steps_transformer import OrderlineStepsTransformer
from src.common.couchdb_client import fetch_data_from_couchdb
from src.common.postgres_loader import load_data_to_postgres

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

with DAG(
    dag_id="etl_processing_dag",
    default_args=default_args,
    description="ETL Processing DAG for CouchDB to PostgreSQL",
    schedule_interval=None,
    start_date=datetime(2023, 11, 1),
    catchup=False,
    tags=["etl", "couchdb", "postgres"],
) as dag:

    @task
    def load_configurations():
        """Load all configurations needed for the ETL process"""
        try:
            schema_config = SchemaLoader.load_schema_config('config/schema/')
            db_params = ConfigLoader.get_postgres_config()
            couchdb_config = ConfigLoader.get_couchdb_config()
            
            return {
                'schema_config': schema_config,
                'db_params': db_params,
                'couchdb_config': couchdb_config
            }
        except Exception as e:
            logging.error(f"Configuration loading failed: {str(e)}")
            raise

    @task.pyspark(
        conn_id="spark-local",
        config_kwargs={
            "spark.kubernetes.driver.request.cores": "0.5",
            "spark.kubernetes.driver.limit.cores": "1",
            "spark.driver.memory": "1g",
            "spark.kubernetes.executor.request.cores": "0.5",
            "spark.kubernetes.executor.limit.cores": "1",
            "spark.executor.memory": "1g",
            "spark.executor.instances": "1",
            "spark.kubernetes.container.image.pullPolicy": "IfNotPresent"
        }
    )
    def process_database(db_name: str, config: dict, spark=None):  # Fixed parameter order
        """Process a single database"""
        try:
            # Initialize transformer based on database name
            transformers = {
                'user': UserTransformer(spark, config['schema_config']),
                'company': CompanyTransformer(spark, config['schema_config']),
                'order': OrderTransformer(spark, config['schema_config']),
                'orderline-steps': OrderlineStepsTransformer(spark, config['schema_config'])
            }
            
            transformer = transformers[db_name]
            db_config = {"name": f"channel1_{db_name}"}
            
            # Fetch and process data
            data = fetch_data_from_couchdb(
                config['couchdb_config']['url'], 
                db_config['name']
            )
            docs = [row['doc'] for row in data]
            
            # Transform data
            transformed_results = transformer.transform(docs)
            
            # Load to PostgreSQL
            for table_name, df in transformed_results:
                sanitized_table_name = table_name.replace('-', '_').lower()
                total_rows = df.count()
                load_data_to_postgres(
                    spark, 
                    df, 
                    config['db_params'], 
                    sanitized_table_name
                )
                
            return f"Successfully processed {db_name}"
            
        except Exception as e:
            logging.error(f"Error processing {db_name}: {str(e)}")
            raise

    # Define task flow
    config = load_configurations()
    
    # Create tasks for each database
    database_tasks = []
    for db_name in ['user', 'company', 'order', 'orderline-steps']:
        database_tasks.append(
            process_database(db_name=db_name, config=config)
        )


    # Set dependencies
    config >> database_tasks
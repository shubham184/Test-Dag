from airflow import DAG
from airflow.decorators import task
from datetime import datetime
import os
import sys
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Add the src directory to Python path
dag_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.join(dag_dir, 'src')
sys.path.append(src_dir)

# Log important directory information
logger.info(f"DAG Directory: {dag_dir}")
logger.info(f"Source Directory: {src_dir}")

# Log directory contents
def log_directory_structure(start_path):
    """Recursively log directory structure"""
    logger.info(f"\nDirectory structure starting from: {start_path}")
    for root, dirs, files in os.walk(start_path):
        level = root.replace(start_path, '').count(os.sep)
        indent = ' ' * 4 * level
        logger.info(f"{indent}{os.path.basename(root)}/")
        sub_indent = ' ' * 4 * (level + 1)
        for f in files:
            logger.info(f"{sub_indent}{f}")

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
    'retries': 0,
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
            # Get absolute paths
            dag_dir = os.path.dirname(os.path.abspath(__file__))
            config_dir = os.path.join(dag_dir, 'config')
            schema_dir = os.path.join(config_dir, 'schema')
            
            logger.info(f"Loading configurations from config directory: {config_dir}")
            
            # Load schema config with absolute path
            schema_config = SchemaLoader.load_schema_config(schema_dir)
            logger.info("Successfully loaded schema config")
            
            # Load database configs with absolute path
            db_params = ConfigLoader.get_postgres_config(config_dir)
            logger.info("Successfully loaded postgres config")
            
            couchdb_config = ConfigLoader.get_couchdb_config(config_dir)
            logger.info("Successfully loaded couchdb config")
            
            return {
                'schema_config': schema_config,
                'db_params': db_params,
                'couchdb_config': couchdb_config
            }
        except Exception as e:
            logger.error(f"Configuration loading failed: {str(e)}")
            logger.error(f"Error type: {type(e)}")
            logger.error("Stack trace:", exc_info=True)
            raise

    # Define task flow
    logger.info("Starting DAG execution")
    config = load_configurations()

    @task
    def extract_from_couchdb(db_name: str, config: dict):
        """Extract data from CouchDB for a given database"""
        try:
            logger.info(f"Extracting data for: {db_name}")
            db_config = {"name": f"channel1_{db_name}"}
            data = fetch_data_from_couchdb(
                config['couchdb_config']['url'], 
                db_config['name']
            )
            return [row['doc'] for row in data]
        except Exception as e:
            logger.error(f"Error extracting data for {db_name}: {str(e)}")
            raise

    @task.pyspark(
        conn_id="spark-local",
        config_kwargs={
            "spark.kubernetes.driver.request.cores": "1",
            "spark.kubernetes.driver.limit.cores": "1",
            "spark.driver.memory": "5g",
            "spark.kubernetes.executor.request.cores": "1",
            "spark.kubernetes.executor.limit.cores": "1",
            "spark.executor.memory": "5g",
            "spark.executor.instances": "1",
            "spark.kubernetes.container.image.pullPolicy": "IfNotPresent",
            "spark.sql.shuffle.partitions": "10",  # Controls number of partitions for shuffles
            "spark.default.parallelism": "4",      # Controls parallelism for operations like joins
            "spark.sql.adaptive.enabled": "true",  # Enables adaptive query execution
            "spark.sql.adaptive.coalescePartitions.enabled": "true",  # Coalesces partitions adaptively
            "spark.sql.adaptive.skewJoin.enabled": "true"  # Handles skewed joins better
        }
    )
    def transform_and_load(db_name: str, docs: list, config: dict, spark=None):
        """Transform and load data to PostgreSQL"""
        try:
            logger.info(f"Transforming and loading data for: {db_name}")
            
            transformer_mapping = {
                'company': CompanyTransformer,
                'order': OrderTransformer,
                'orderline-steps': OrderlineStepsTransformer,
                'user': UserTransformer
            }
            
            if db_name not in transformer_mapping:
                raise ValueError(f"Unknown database type: {db_name}")
            
            transformer = transformer_mapping[db_name](spark, config['schema_config'])
            transformed_results = transformer.transform(docs)
            
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
            logger.error(f"Error in transform and load for {db_name}: {str(e)}")
            raise

    # Create dynamic tasks
    extraction_tasks = []
    transform_load_tasks = []

    for db_name in ['user', 'company', 'order', 'orderline-steps']:
    # for db_name in ['order']:
        logger.info(f"Creating tasks for database: {db_name}")
        
        # Create extraction task
        extract_task = extract_from_couchdb.override(
            task_id=f"extract_{db_name}_data"
        )(db_name=db_name, config=config)
        
        # Create transform and load task
        transform_load_task = transform_and_load.override(
            task_id=f"transform_load_{db_name}"
        )(db_name=db_name, docs=extract_task, config=config)
        
        # Set dependencies for this database's pipeline
        config >> extract_task >> transform_load_task
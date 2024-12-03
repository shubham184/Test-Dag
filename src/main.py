import logging
from common.spark_session import create_spark_session
from common.couchdb_client import fetch_data_from_couchdb
from common.postgres_loader import load_data_to_postgres
from common.config_loader import ConfigLoader
from models.schema_loader import SchemaLoader
from transformers.user_transformer import UserTransformer
from transformers.company_transformer import CompanyTransformer
from transformers.order_transformer import OrderTransformer
from transformers.orderline_steps_transformer import OrderlineStepsTransformer
from transformers.order_fabric_footprint_transformer import OrderFabricFootprintTransformer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    """
    Main function to initialize Spark, load configurations, process data from CouchDB, 
    transform it, and load it into PostgreSQL.
    Steps:
    1. Initialize Spark session.
    2. Load schema configurations and database parameters.
    3. Initialize data transformers for each database.
    4. For each database:
        a. Fetch data from CouchDB.
        b. Extract documents from the fetched data.
        c. Transform the documents using the corresponding transformer.
        d. Load the transformed data into PostgreSQL.
    5. Handle and log any errors that occur during processing.
    6. Stop the Spark session.
    Raises:
        Exception: If there is an error during any step of the process.
    """
    # Initialize Spark
    spark = create_spark_session()
    
    try:
        # Load configurations
        schema_config = SchemaLoader.load_schema_config('pyspark_etl_project/config/schema/')
        db_params = ConfigLoader.get_postgres_config()
        couchdb_config = ConfigLoader.get_couchdb_config()
        
        # Initialize transformers
        transformers = {
            # 'user': UserTransformer(spark, schema_config),
            # 'company': CompanyTransformer(spark, schema_config),
            'order': OrderTransformer(spark, schema_config),
            # 'orderline-steps': OrderlineStepsTransformer(spark, schema_config)
            # 'fabric_fabricFootprint': OrderFabricFootprintTransformer(spark, schema_config)
        }
        
        # Process each database
        for db_name, transformer in transformers.items():
            db_config = {"name": f"channel1_{db_name}"}
            try:
                # Fetch data and extract docs
                data = fetch_data_from_couchdb(couchdb_config['url'], db_config['name'])
                # data = fetch_data_from_couchdb(couchdb_config['url'], 'channel1_order')
                docs = [row['doc'] for row in data]  # Extract docs here
                
                # Transform data
                transformed_results = transformer.transform(docs)# Pass docs directly
                # Load to PostgreSQL
                for table_name, df in transformed_results:
                    sanitized_table_name = table_name.replace('-', '_').lower()
                    total_rows = df.count()
                    load_data_to_postgres(spark, df, db_params, sanitized_table_name)
                    
            except Exception as e:
                logging.error(f"Error processing {db_config['name']}: {str(e)}")
                raise
                
    except Exception as e:
        logging.error(f"Application error: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
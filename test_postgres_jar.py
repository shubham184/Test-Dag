from airflow import DAG
from airflow.decorators import task
from datetime import datetime
from pyspark.sql import SparkSession

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

with DAG(
    dag_id="test_spark_jdbc_session",
    default_args=default_args,
    description="Test Spark Session with JDBC driver",
    schedule_interval=None,
    start_date=datetime(2023, 11, 1),
    catchup=False,
    tags=["test"],
) as dag:

    @task.pyspark(
        conn_id="spark-local",
        config_kwargs={
            # Resource configs
            "spark.kubernetes.driver.request.cores": "0.5",
            "spark.kubernetes.driver.limit.cores": "1",
            "spark.driver.memory": "4g",
            "spark.kubernetes.executor.request.cores": "0.5",
            "spark.kubernetes.executor.limit.cores": "1",
            "spark.executor.memory": "4g",
            "spark.executor.instances": "1",
            
            # JDBC driver configuration - Spark will handle the distribution
            "spark.jars": "https://jdbc.postgresql.org/download/postgresql-42.7.3.jar",
            
            # Your existing spark configs
            "spark.sql.shuffle.partitions": "10",
            "spark.default.parallelism": "10",
            "spark.sql.autoBroadcastJoinThreshold": "10m",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",
            "spark.sql.inMemoryColumnarStorage.compressed": "true",
            "spark.sql.shuffle.compress": "true",
            "spark.network.timeout": "800s",
            "spark.executor.heartbeatInterval": "60s"
        }
    )
    def test_spark_jdbc(spark: SparkSession) -> None:
        print("Testing Spark session with JDBC driver...")
        
        # Print Spark configuration
        print("\nSpark Configuration:")
        for item in spark.sparkContext.getConf().getAll():
            print(f"{item[0]}: {item[1]}")
        
        # Create and test a simple DataFrame
        test_df = spark.createDataFrame([
            (1, "Test1"),
            (2, "Test2")
        ], ["id", "name"])
        
        print("\nTest DataFrame:")
        test_df.show()
        
        # Try to load the JDBC driver class to verify it's available
        try:
            spark.sparkContext._jvm.Class.forName("org.postgresql.Driver")
            print("\nSuccessfully loaded PostgreSQL JDBC driver!")
        except Exception as e:
            print(f"\nError loading PostgreSQL JDBC driver: {str(e)}")
        
        return "Success!"

    test_task = test_spark_jdbc()

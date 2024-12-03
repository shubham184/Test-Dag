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
            
            # Init container to download JDBC driver
            "spark.kubernetes.driver.initContainers": '[{"name": "init-jdbc", "image": "curlimages/curl", "command": ["curl", "-o", "/opt/spark/jars/postgresql-42.7.3.jar", "https://jdbc.postgresql.org/download/postgresql-42.7.3.jar"], "volumeMounts": [{"name": "spark-jars", "mountPath": "/opt/spark/jars"}]}]',
            "spark.kubernetes.executor.initContainers": '[{"name": "init-jdbc", "image": "curlimages/curl", "command": ["curl", "-o", "/opt/spark/jars/postgresql-42.7.3.jar", "https://jdbc.postgresql.org/download/postgresql-42.7.3.jar"], "volumeMounts": [{"name": "spark-jars", "mountPath": "/opt/spark/jars"}]}]',
            
            # Volume mounts for the downloaded jar
            "spark.kubernetes.driver.volumes.emptyDir.spark-jars.mount.path": "/opt/spark/jars",
            "spark.kubernetes.executor.volumes.emptyDir.spark-jars.mount.path": "/opt/spark/jars",
            
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
        
        # Verify JDBC driver is available
        import subprocess
        print("Listing files in /opt/spark/jars:")
        subprocess.run(["ls", "-l", "/opt/spark/jars"])
        
        # Create a test DataFrame
        test_df = spark.createDataFrame([
            (1, "Test1"),
            (2, "Test2")
        ], ["id", "name"])
        
        test_df.show()
        return "Success!"

    test_task = test_spark_jdbc()

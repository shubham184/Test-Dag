from airflow import DAG
from airflow.decorators import task
from datetime import datetime
from pyspark.sql import SparkSession

default_args = {
    'owner': 'airflow',
    'retries': 1,
}

with DAG(
    'test_spark_jdbc_session',
    default_args=default_args,
    start_date=datetime(2023, 11, 1),
    catchup=False,
) as dag:

    @task.pyspark(
        conn_id="spark-local",
        config_kwargs={
            # Basic resource configs
            "spark.driver.memory": "1g",
            "spark.executor.memory": "1g",
            "spark.executor.instances": "1",
            
            # Volume mounts
            "spark.kubernetes.driver.volumes.persistentVolumeClaim.spark-jars.mount.path": "/opt/spark/jars",
            "spark.kubernetes.driver.volumes.persistentVolumeClaim.spark-jars.mount.name": "spark-jars-pvc",
            "spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-jars.mount.path": "/opt/spark/jars",
            "spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-jars.mount.name": "spark-jars-pvc"
        }
    )
    def test_spark_jdbc(spark: SparkSession) -> None:
        print("Testing Spark session...")
        
        # List the contents of the mounted directory
        import subprocess
        print("Contents of /opt/spark/jars:")
        subprocess.run(["ls", "-l", "/opt/spark/jars"])
        
        # Simple test
        df = spark.createDataFrame([(1, "test")], ["id", "value"])
        df.show()
        
        return "Success!"

    test_task = test_spark_jdbc()

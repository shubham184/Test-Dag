from airflow import DAG
from airflow.decorators import task
from datetime import datetime
from pyspark.sql import SparkSession
import os

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
            # "spark.jars.packages": "org.postgresql:postgresql:42.7.3",
            # "spark.driver.extraClassPath": "/home/airflow/.ivy2/jars/org.postgresql_postgresql-42.7.3.jar",
            "spark.driver.extraClassPath": "/opt/spark-jars/postgresql-42.7.3.jar",
            "spark.executor.extraClassPath": "/opt/spark-jars/postgresql-42.7.3.jar",
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
    def test_spark_jdbc(spark: SparkSession) -> None:
        print("Testing Spark session with JDBC driver...")

        # Test JDBC driver loading
        try:
            spark.sparkContext._jvm.Class.forName("org.postgresql.Driver")
            print("Successfully loaded PostgreSQL JDBC driver!")
        except Exception as e:
            print(f"Error loading PostgreSQL JDBC driver: {str(e)}")

        # Simple test
        df = spark.createDataFrame([(1, "test")], ["id", "value"])
        df.show()

        return "Success!"

    test_task = test_spark_jdbc()

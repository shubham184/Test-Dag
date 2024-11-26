from airflow import DAG
from airflow.decorators import task
from datetime import datetime
from pyspark.sql import SparkSession

# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

with DAG(
    dag_id="example_pyspark_dag",
    default_args=default_args,
    description="Simple DAG using PySpark",
    schedule_interval=None,  # Run manually
    start_date=datetime(2023, 11, 1),
    catchup=False,
    tags=["example"],
) as dag:

    @task.pyspark(
        conn_id="spark-local",
        config_kwargs={
            "spark.driver.memory": "1g",
            "spark.executor.memory": "1g",
            "spark.executor.cores": "1",
            "spark.python.version": "3"
        }
    )
    def spark_task(spark: SparkSession) -> None:
        # Create a simple DataFrame
        df = spark.createDataFrame(
            [
                (1, "John Doe", 21),
                (2, "Jane Doe", 22),
                (3, "Joe Bloggs", 23),
            ],
            ["id", "name", "age"],
        )
        # Show DataFrame content in logs
        df.show()
        return "Success!"

    # Define task in the DAG
    task1 = spark_task()

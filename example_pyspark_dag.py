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
    dag_id="example_pyspark_dag",
    default_args=default_args,
    description="Simple PySpark on K8s DAG",
    schedule_interval=None,
    start_date=datetime(2023, 11, 1),
    catchup=False,
    tags=["example"],
) as dag:

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
    def spark_task(spark: SparkSession) -> None:
        print("Starting Spark task...")
        df = spark.createDataFrame(
            [
                (1, "John Doe", 21),
                (2, "Jane Doe", 22),
                (3, "Joe Bloggs", 23),
            ],
            ["id", "name", "age"],
        )
        print("Created DataFrame:")
        df.show()
        print("Task completed successfully!")
        return "Success!"

    task1 = spark_task()

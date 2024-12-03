import os
from pyspark.sql import SparkSession

def create_spark_session():
    # Get the absolute path to the PostgreSQL JDBC driver JAR
    jdbc_jar_path = os.path.abspath("postgresql-42.7.3.jar")
    
    return (SparkSession.builder
        .appName("CouchDB to PostgreSQL ETL")
        # JDBC driver configuration
        .config("spark.driver.extraClassPath", jdbc_jar_path)
        .config("spark.executor.extraClassPath", jdbc_jar_path)
        # Memory configuration
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        # Partition tuning
        .config("spark.sql.shuffle.partitions", "10")  # Reduce from default 200
        .config("spark.default.parallelism", "10")     # Match shuffle partitions
        # Broadcast threshold - increase for larger datasets
        .config("spark.sql.autoBroadcastJoinThreshold", "10m")
        # Serialization
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        # Task size configuration
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        # Compression for shuffle operations
        .config("spark.sql.inMemoryColumnarStorage.compressed", "true")
        .config("spark.sql.shuffle.compress", "true")
        # Network timeout
        .config("spark.network.timeout", "800s")
        .config("spark.executor.heartbeatInterval", "60s")
        .getOrCreate())
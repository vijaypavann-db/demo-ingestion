# from pyspark.sql import SparkSession
from databricks.connect import DatabricksSession
from databricks.sdk.core import Config
    
if __name__ == "__main__":
    
    config = Config(profile = "DEV")
    spark = DatabricksSession.builder.sdkConfig(config).getOrCreate()

    # query = "show tables from default1" # demo_metadata
    # query = "show databases"
    # query = "create database demo_metadata"
    query = "select * from default1.operational_status" # demo_metadata
    
    df = spark.sql(query)
    df.show(truncate = False)
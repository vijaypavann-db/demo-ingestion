from pyspark.sql import SparkSession

if __name__ == "__main__":
    
    spark = SparkSession.builder.getOrCreate()

    query = "show tables from default1" # demo_metadata
   #  query = "show databases"
   #  query = "create database demo_metadata"
   #  query = "SELECT * FROM demo_metadata.Entity_Runs;"

    # query = "USE default1" 
    df = spark.sql(query)
    df.show()
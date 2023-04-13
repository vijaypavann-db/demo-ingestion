from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

if __name__ == "__main__":
    
    builder = (SparkSession.builder.master("local[*]").appName("Local Metadata Setup")
                   # .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                   # .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                #    .config("spark.sql.warehouse.dir", "spark-warehouse")
                #    .config("hive.metastore.warehouse.dir", "file:/Users/vijay.pavan/PycharmProjects/ingestion-fw/demo-ingestion/spark-warehouse")
                   )

    spark: SparkSession = spark # builder.getOrCreate() # configure_spark_with_delta_pip(builder).enableHiveSupport().getOrCreate()

    # print(spark.conf.get("spark.sql.warehouse.dir"), spark.conf.get("hive.metastore.warehouse.dir"))

    # df = spark.sql("CREATE TABLE dummy (id INT) USING DELTA")
    # df.show()

    # query = "show tables from demo_metadata"
    # query = "show databases"
    # query = "create database demo_metadata"
    query = "SELECT * FROM demo_metadata.Entity_Runs;"
    df = spark.sql(query)
    df.show()
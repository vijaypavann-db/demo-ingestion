from pyspark.sql import SparkSession
from com.db.fw.metadata.setup import MetadataSetup
from delta import configure_spark_with_delta_pip

if __name__ == "__main__":

    builder = (SparkSession.builder
               .master("local[*]")
               .appName("Local Metadata Setup")
               .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
               .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
               )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    setup = MetadataSetup(spark, db_name = "default1", debug = True, dropTables = True)
    
    # Create all Tables
    setup.run()

    # Run for a single table
    # setup.createTable("pipeline_metadata")
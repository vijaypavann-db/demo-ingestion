from pyspark.sql import SparkSession
from com.db.fw.metadata.setup import MetadataSetup

if __name__ == "__main__":

    spark = SparkSession.builder.getOrCreate() 

    setup = MetadataSetup(spark, db_name = "default1", debug = True, dropTables = True)

    # Create all Tables
    setup.run()

    # Run for a single table
    # setup.createTable("pipeline_metadata")
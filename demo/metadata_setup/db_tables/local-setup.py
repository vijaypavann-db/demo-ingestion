from pyspark.sql import SparkSession
from com.db.fw.metadata.setup import MetadataSetup
from delta import configure_spark_with_delta_pip
import getpass
from com.db.fw.etl.core.common.Constants import *

if __name__ == "__main__":

    username = getpass.getuser()
    builder = (SparkSession.builder
               .master("local[*]")
               .appName("Local Metadata Setup")
               .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
               .config("spark.sql.warehouse.dir", f"/Users/{username}/spark-warehouse")
               .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
               .enableHiveSupport()
               )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # Add db_name = "<NEW_DB>", if you would like to change the default COMMON_CONSTANTS.METADATA_DB [demo_metadata]
    setup = MetadataSetup(spark, debug = True, dropTables = True, localRun = True)
    
    # Create all Tables
    setup.run()

    # Run for a single table
    # setup.createTable("pipeline_metadata")
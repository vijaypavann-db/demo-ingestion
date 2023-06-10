from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import getpass

from com.db.fw.etl.core.pipeline.DeltaFromYaml import PrepareDelta
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

    pipeline_metadata_path = "./resources/pipeline_metadata/local/1stload_local"
    obj = PrepareDelta(spark, pipeline_metadata_path)
    obj.start()
    
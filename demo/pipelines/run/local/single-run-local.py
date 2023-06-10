from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import getpass

from com.db.fw.etl.core.pipeline.PipelineBuilder import *
from com.db.fw.etl.core.pipeline.PipelineUtils import PipelineUtils

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

    sparkSession = configure_spark_with_delta_pip(builder).getOrCreate()

    utils = PipelineUtils(spark = sparkSession, localRun = True)
    # run_id = "4cae5de8-5771-47de-bc1d-eae0daf84ab8"
    # pipeline = utils.buildPipelineUsingRunId(run_id)
    
    pipeline_id = "1964147365"
    pipeline = utils.buildPipeline(pipeline_id)

    # pipline_id, name, meta_jsons = PipelineBuilder.get_json_dump(pipeline)
    # print("*******", pipline_id, name, meta_jsons)
    pipeline.start() # run_id = "a17991ee-fe03-11ed-8182-aa665a13f2b0"

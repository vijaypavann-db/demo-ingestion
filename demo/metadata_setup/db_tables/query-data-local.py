from pyspark.sql import SparkSession
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

    # spark.sql("use demo_metadata")
    # spark.sql("show tables").show(20, False)

    # `pipeline_tasks`
    # qry = "DROP TABLE `demo_metadata`.`pipeline_dependencies` "
    # qry = "DROP TABLE `demo_metadata`.`pipeline_tasks` "
    # print(qry, type(qry))
    # spark.sql(qry).show(20, False)

    # qry = """ UPDATE demo_metadata.pipeline_tasks SET task_options = map("delimiter", "|", "header", "true") 
    #             WHERE task_name = "emp_reader" """
    #             # task_id = "d36cc211-6e67-492e-8541-e2a6f1c08653"
    # print(qry, type(qry))
    # spark.sql(qry).show(20, False)

    # qry = "SELECT * FROM `demo_metadata`.`pipeline_tasks` "
    # print(qry, type(qry))
    # spark.sql(qry).show(20, False)

    qry = "DROP DATABASE `demo_poc` CASCADE "
    print(qry, type(qry))
    spark.sql(qry).show(20, False)
    
    qry = "CREATE DATABASE IF NOT EXISTS `demo_poc` "
    print(qry, type(qry))
    spark.sql(qry).show(20, False)

    # data = "./resources/data/tmp/export.csv"
    # df = spark.read.format("csv").option("header", "True").load(data)
    # df.show(2, False)

    # out = "./resources/data/lc/"
    # # .option("overwriteSchema", "true")
    # df.write.format("parquet").mode("overwrite").save(out)

    # df = spark.read.format("parquet").load(out)
    # df.show(2, False)
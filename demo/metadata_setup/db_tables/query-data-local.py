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
    spark.sparkContext.setLogLevel("ERROR")  

    db = 'demo_metadata' #  demo_metadata  demo_poc
    spark.sql(f"use {db}") 
    # spark.sql("show tables").show(20, False)

    # `pipeline_tasks`
    # qry = "DROP TABLE `demo_metadata`.`pipeline_dependencies` "
    # qry = "DROP TABLE `demo_metadata`.`pipeline_tasks` "
    # qry = "DROP TABLE `lending_club_agg` "
    # print(qry, type(qry))
    # spark.sql(qry).show(20, False)

    # qry = """ UPDATE pipeline_tasks SET task_options = map("delimiter", "|", "header", "true") 
    #             WHERE task_name = "emp_reader" 
    #             AND pipeline_id = '-1903234029' """
    #             # task_id = "d36cc211-6e67-492e-8541-e2a6f1c08653"
    # print(qry, type(qry))
    # spark.sql(qry).show(20, False)

    select_stmt = """select lcr.grade, lcr.count, emp.id, emp.name FROM 
                        (select grade, count(*) AS count from lc_writer where grade is not null group by grade) lcr 
                      INNER JOIN emp_reader emp
                      ON lcr.grade = emp.grade
                  """
    qry = f""" UPDATE pipeline_tasks SET task_configs = map("type", "processor", "custom_sql", "{select_stmt}") 
                WHERE task_name = "emp_lc_processor" 
                AND pipeline_id = '1159794704'
           """
                # task_id = "d36cc211-6e67-492e-8541-e2a6f1c08653"
    print(qry, type(qry))
    spark.sql(qry).show(20, False)

    
    # qry = "SELECT * FROM `pipeline_tasks` where pipeline_name = 'recovery_pipeline_fail' "
    # print(qry, type(qry))
    # spark.sql(qry).show(20, False)

    # db = 'demo_poc_fail' #  demo_metadata
    # qry = f"DROP DATABASE IF EXISTS `{db}` CASCADE "
    # print(qry, type(qry))
    # spark.sql(qry).show(20, False)
    
    # qry = f"CREATE DATABASE IF NOT EXISTS `{db}` "
    # print(qry, type(qry))
    # spark.sql(qry).show(20, False)

    # data = "./resources/data/tmp/export.csv"
    # df = spark.read.format("csv").option("header", "True").load(data)
    # df.show(2, False)

    # out = "./resources/data/lc/"
    # # .option("overwriteSchema", "true")
    # df.write.format("parquet").mode("overwrite").save(out)

    # df = spark.read.format("parquet").load(out)
    # df.show(2, False)


    # spark.sql("use demo_metadata").show()
    
    # # qry = "SELECT * FROM `pipeline_status` "

    # qry = """ SELECT status FROM `task_status` 
    #                 where 
    #                     date(updated_at) = current_date() AND 
    #                     run_id = '74489328-0dd5-11ee-aa30-acde48001122' AND
    #                     pipeline_id = '-411812449' AND
    #                     task_name = 'lc_writer'
    #                 ORDER BY updated_at DESC
    #                 """
                
    # print(qry, type(qry))
    # df = spark.sql(qry).cache()
    # print(df, type(df), df.isEmpty())
    # df.show(20, False)

    # print(df.head()["status"] == "finished")

    # qry = f"SELECT * FROM demo_rec.lending_club"
    # print(qry, type(qry))
    # df = spark.sql(qry)
    # df.printSchema()

    # df.show(20, False)
    

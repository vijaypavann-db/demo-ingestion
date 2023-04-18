from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

if __name__ == "__main__":
    
    builder = (SparkSession.builder
                   .master("local[*]")
                   .appName("Local Metadata Setup")
                   .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                   .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                   # .config("spark.sql.warehouse.dir", "spark-warehouse")
                   # .config("hive.metastore.warehouse.dir", "file:/Users/vijay.pavan/PycharmProjects/ingestion-fw/demo-ingestion/spark-warehouse")
                   )

    spark: SparkSession = configure_spark_with_delta_pip(builder).enableHiveSupport().getOrCreate()

    # print(spark.conf.get("spark.sql.warehouse.dir"), spark.conf.get("hive.metastore.warehouse.dir"))

    # df = spark.sql("CREATE TABLE dummy (id INT) USING DELTA")
    # df.show()

    # query = "show tables from default1" # demo_metadata
    # query = "show databases"
    # query = "create database demo_metadata"
    # query = "SELECT * FROM demo_metadata.Entity_Runs;"

    query = "USE default1" 
    df = spark.sql(query)
    df.show()


   #  pipeline_metadata_insert = """INSERT INTO pipeline_metadata 
   #                         (pipeline_id,
   #                         pipeline_name,
   #                         db_name,
   #                         entity_name,
   #                         pipeline_type,
   #                         insert_time,
   #                         updated_at)
   #                         SELECT 
   #                            uuid(),
   #                               "load_emp",
   #                               "demo_ingestion",
   #                               "EMP",
   #                               "BATCH_LOAD",
   #                               current_timestamp(),
   #                               current_timestamp()
   #                               ;"""

   #  df = spark.sql(pipeline_metadata_insert)
   #  df.show()

    pipeline_metadata_select = f"""SELECT * FROM pipeline_metadata 
                            WHERE pipeline_name = 'load_emp' AND
                           db_name = 'demo_ingestion' AND
                           entity_name = 'EMP'
                           ORDER BY updated_at DESC 
                           LIMIT 1; 
                           """
    df = spark.sql(pipeline_metadata_select)
    df.show()
    pipeline_id = df.first()["pipeline_id"]
   
 

    pipeline_options_insert = f"""INSERT INTO pipeline_options 
                                    (id,
                                    pipeline_id,
                                    reader_configs,
                                    reader_options,
                                    processor_configs,
                                    processor_options,
                                    writer_configs,
                                    writer_options,
                                    node_info,
                                    insert_time,
                                    is_active) 
                                    SELECT
                                          uuid(),
                                          '{pipeline_id}',
                                          map("path", "/tmp/inp.csv", "format", "csv"),
                                          map("delimiter", "|", "header", "true"),
                                          map(), 
                                          map(), 
                                          map("mode", "append"),
                                          map("partiton_cols", "state|city"),
                                          null,
                                          CURRENT_TIMESTAMP(),
                                          TRUE
                                       ;"""
    
   #  df = spark.sql(pipeline_options_insert)
   #  df.show()
    

    pipeline_options_select = f"""SELECT * FROM pipeline_options 
                            WHERE pipeline_id = '{pipeline_id}'
                            AND is_active = True
                           ORDER BY insert_time DESC 
                           LIMIT 1; 
                           """
    df = spark.sql(pipeline_options_select)
    df.show()
    print(df.head()["id"])
    
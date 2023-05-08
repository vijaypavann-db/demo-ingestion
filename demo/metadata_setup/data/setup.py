from pyspark.sql import SparkSession


def insert_into_pipeline_metadata(pipeline_name, db_name, entity_name, pipeline_type) -> str:
     return f"""INSERT INTO pipeline_metadata 
                           (pipeline_id,
                           pipeline_name,
                           db_name,
                           entity_name,
                           pipeline_type,
                           insert_time,
                           updated_at)
                     SELECT 
                           uuid(),
                           '{pipeline_name}',
                           '{db_name}',
                           '{entity_name}',
                           '{pipeline_type}',   
                           current_timestamp(),
                           current_timestamp()
                           ;"""

def default_if_none(dict):
     if dict is None:
          return "map()"
     else: 
         return dict    
     

def insert_into_pipeline_options(pipeline_id, reader_configs, reader_options, 
                                 processor_configs, processor_options, 
                                 writer_configs, writer_options) -> str:
     
     reader_configs = default_if_none(reader_configs)
     reader_options = default_if_none(reader_options)

     processor_configs = default_if_none(processor_configs)
     processor_options = default_if_none(processor_options)
     
     writer_configs = default_if_none(writer_configs)
     writer_options = default_if_none(writer_options)

     return f"""INSERT INTO pipeline_options 
                                    (run_id,
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
                                          {reader_configs},
                                          {reader_options},
                                          {processor_configs},
                                          {processor_options},
                                          {writer_configs},
                                          {writer_options},
                                          null,
                                          CURRENT_TIMESTAMP(),
                                          TRUE
                                       ;"""


if __name__ == "__main__":
    
    spark = SparkSession.builder.getOrCreate()

    query = "USE default1" 
    df = spark.sql(query)
    df.show()


    # pipeline_metadata_insert = insert_into_pipeline_metadata("load_emp", "demo_ingestion", "EMP", "BATCH_LOAD")
    pipeline_metadata_insert1 = insert_into_pipeline_metadata("load_readme", "demo_ingestion", "README", "BATCH_LOAD")
    pipeline_metadata_insert2 = insert_into_pipeline_metadata("load_people", "demo_ingestion", "people", "BATCH_LOAD")
    pipeline_metadata_insert3 = insert_into_pipeline_metadata("load_data_geo", "demo_ingestion", "data_geo", "BATCH_LOAD")
    pipeline_metadata_insert4 = insert_into_pipeline_metadata("load_lending_club", "demo_ingestion", "lending_club", "BATCH_LOAD")
   
   #  df = spark.sql(pipeline_metadata_insert)
   #  df.show()

   #  for qry in [pipeline_metadata_insert1, pipeline_metadata_insert2, pipeline_metadata_insert3, pipeline_metadata_insert4]:
   #    print(qry)
   #    df = spark.sql(qry)
   #    df.show()

    pipeline_metadata_select = f"""SELECT * FROM pipeline_metadata 
                                    WHERE pipeline_name = 'load_emp' AND
                                    db_name = 'demo_ingestion' AND
                                    entity_name = 'EMP'
                                    ORDER BY updated_at DESC 
                                    LIMIT 1; 
                                    """
    
   #  pipeline_metadata_select = f"""SELECT * FROM pipeline_metadata 
   #                                  WHERE pipeline_name = 'load_readme' AND
   #                                  db_name = 'demo_ingestion' AND
   #                                  entity_name = 'README'
   #                                  ORDER BY updated_at DESC 
   #                                  LIMIT 1; 
   #                                  """
  
    
#     pipeline_metadata_select = f"""SELECT * FROM pipeline_metadata 
#                                     WHERE pipeline_name = 'load_lending_club' AND
#                                     db_name = 'demo_ingestion' AND
#                                     entity_name = 'lending_club'
#                                     ORDER BY updated_at DESC 
#                                     LIMIT 1; 
#                                     """
  
    df = spark.sql(pipeline_metadata_select)
    df.show()
    pipeline_id = df.first()["pipeline_id"]

   #  pipeline_options_insert = insert_into_pipeline_options( pipeline_id,
   #                                                          reader_configs = map("path", "/tmp/inp.csv", "format", "csv"),
   #                                                          reader_options = map("delimiter", "|", "header", "true"),
   #                                                          processor_configs = None, 
   #                                                          processor_options = None, 
   #                                                          writer_configs = map("mode", "append"),
   #                                                          writer_options = map("partiton_cols", "state|city")
   #                                                          )


   #  pipeline_options_insert = insert_into_pipeline_options( pipeline_id,
   #                                                          reader_configs = """map("path", "/databricks-datasets/samples/docs/README.md", "format", "text") """,
   #                                                          reader_options = None,
   #                                                          processor_configs = None, 
   #                                                          processor_options = None, 
   #                                                          writer_configs = """map("mode", "append")""",
   #                                                          writer_options = None
   #                                                          )
    
   #  pipeline_options_insert = insert_into_pipeline_options( pipeline_id,
   #                                                          reader_configs = """map("path", "/databricks-datasets/samples/docs/README.md", "format", "text") """,
   #                                                          reader_options = None,
   #                                                          processor_configs = None, 
   #                                                          processor_options = None, 
   #                                                          writer_configs = """map("mode", "append")""",
   #                                                          writer_options = None
   #                                                          )

    pipeline_options_insert = insert_into_pipeline_options( pipeline_id,
                                                            reader_configs = """map("path", "/databricks-datasets/samples/lending_club/parquet/", "format", "parquet") """,
                                                            reader_options = None,
                                                            processor_configs = None, 
                                                            processor_options = None, 
                                                            writer_configs = """map("mode", "overwrite")""",
                                                            writer_options = None
                                                            )
    
#     print(pipeline_options_insert)

#     df = spark.sql(pipeline_options_insert)
#     df.show()
    

    pipeline_options_select = f"""SELECT * FROM pipeline_options 
                            WHERE pipeline_id = '{pipeline_id}'
                            AND is_active = True
                           ORDER BY insert_time DESC; 
                           """
    df = spark.sql(pipeline_options_select)
    df.show()
    print(df.head()["run_id"])
    
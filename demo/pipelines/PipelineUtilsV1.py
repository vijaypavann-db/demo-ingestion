import logging
import uuid

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import *

from com.db.fw.etl.core.pipeline.PipelineBuilder import *
from com.db.fw.etl.core.common.DeltaStatLogger import IOService
from com.db.fw.etl.core.common.Constants import COMMON_CONSTANTS

class PipelineUtils:

    metadata_db = COMMON_CONSTANTS.METADATA_DB
    
    pipeline_metadata_tbl = COMMON_CONSTANTS.PIPELINE_METADATA_TABLE
    pipeline_options_tbl = COMMON_CONSTANTS.PIPELINE_OPTIONS_TABLE

    def __init__(self):
        self.spark = SparkSession.builder.getOrCreate()
    
    def buildPipelineUsingRunId(self, run_id: str):
        
        options = (self.spark.read.table(f"{self.metadata_db}.{self.pipeline_options_tbl}")
                        .filter(f"run_id == '{run_id}'") ).alias("opt")
        
        metadata = self.spark.read.table(f"{self.metadata_db}.{self.pipeline_metadata_tbl}").alias("met")
        
        pipeline_details = (metadata
                            .join(options, metadata["pipeline_id"] == options["pipeline_id"], "inner")
                            .drop(options["pipeline_id"])
                            .head())
        
        print(pipeline_details)

        return self.pipelineBuilder(pipeline_details)

    def pipelineBuilder(self, pipeline_details: Row): 

        # Pipeline Info       
        pipeline_id = pipeline_details["pipeline_id"]
        run_id = pipeline_details["run_id"]

        # Entity Info
        database = pipeline_details["db_name"]
        entity_name = pipeline_details["entity_name"]
        
        # reader_options = pipeline_details["reader_options"]
        # delimiter = reader_options.get("delimiter", ",")

        # writer_options = pipeline_details["writer_options"]
        # partition_keys = writer_options.get("partition_cols", "").lower()

        # Run Info
        # reader_configs = pipeline_details["reader_configs"]
        # file_format = reader_configs["format"]
        # input_path = reader_configs.get("path", "")

        # writer_configs = pipeline_details["writer_configs"]
        # write_mode = writer_configs.get("mode", "append")

        # TODO add Loggers for Options

        # Reader Configs
        # reader_configs = {"file_format": file_format, "input_path": input_path}

        # Reader Options
        # reader_options = None
        # if file_format == "csv":
        #     csv_options = {"delimiter": delimiter}
        #     if reader_options.get("header") == "true":
        #         csv_options = csv_options | {"header": "true"}
        #     else:
        #         # TODO read schema from headerPath / headerCols & add the datatype info
        #         csv_options = csv_options | { "schema": "true"}
        #     reader_options = csv_options

        # elif file_format == "json":        
        #     # TODO specify json options here
        #     json_options = {}
        #     reader_options = json_options
        # elif file_format == "snowflake":
        #     reader_options = pipeline_details

        reader_configs = pipeline_details['reader_configs']
        reader_options = pipeline_details['reader_options']
        file_format = reader_configs["format"]

        print(f"reader_configs {reader_configs} \n reader_options {reader_options} ")
        
        # Processor Options
        # processor_options = entity_details
        print(f"processor_options {pipeline_details['processor_options']}")

        # Writer Options
        # writer_configs = {"mode": write_mode, "database": database, "entity_name": entity_name}
        
        # # Writer Options
        # writer_options = None
        # if partition_keys:
        #     # TODO add other writer options
        #     writer_options = {"partition_keys": partition_keys}

        # print(f"writer_configs {writer_configs} \n writer_options {writer_options}")

        file_sources = ["csv", "json", "text", "parquet", "avro", "orc"]

        task = PipelineNodeBuilder.SAMPLE_DF_READER
        if file_format.lower() in file_sources:
            task = PipelineNodeBuilder.GENERIC_BATCH_READER
            
        reader = (PipelineNodeBuilder() 
                    .set_name(f"{file_format}_df_reader_{run_id}") 
                    .set_type(task) 
                    .add_input_option("configs", reader_configs)
                    .add_input_option("options", reader_options)
                    .build() )

        writer_configs = pipeline_details['writer_configs']
        writer_options = pipeline_details['writer_options']
        
        writer_configs = writer_configs | {COMMON_CONSTANTS.DB_NAME : database, 
                                           COMMON_CONSTANTS.TABLE_NAME : entity_name}
        
                    
        writer = (PipelineNodeBuilder() 
                    .set_name(f"delta_writer_{run_id}") 
                    .set_type(PipelineNodeBuilder.DELTA_WRITER) 
                    .add_input_option(COMMON_CONSTANTS.OPTIONS, writer_options)
                    .add_input_option(COMMON_CONSTANTS.CONFIGS, writer_configs)
                    .build() )

        io_service = IOService()
        logger = logging.getLogger(__name__)

        pipeline = (PipelineBuilder(self.spark, entity_name, logger, pipeline_id, io_service) 
                    .add_node(reader) 
                    # .add_node_after(reader.name, processor)
                    .add_node_after(reader.name, writer).build() )

        pip_id,name,meta_jsons = PipelineBuilder.get_json_dump(pipeline)
        print(pip_id)
        print(name)
        print(meta_jsons)

        # TODO: Store meta_jsons in `pipeline_options` table
        return pipeline

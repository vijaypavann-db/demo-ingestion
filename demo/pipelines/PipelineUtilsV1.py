import logging

from pyspark.sql import SparkSession, Row

from com.db.fw.etl.core.pipeline.PipelineBuilder import *
from com.db.fw.etl.core.common.DeltaStatLogger import IOService
from com.db.fw.etl.core.common.Constants import COMMON_CONSTANTS

class PipelineUtils:

    metadata_db = COMMON_CONSTANTS.METADATA_DB
    
    pipeline_tasks_tbl = COMMON_CONSTANTS.TASKS_TABLE    
    pipeline_dependencies_tbl = COMMON_CONSTANTS.DEPENDENCIES_TABLE

    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def buildPipeline(self, pipeline_id: str):
        """
            Reads the `Pipeline Metadata` from `Pipeline Tables` and triggers the `Pipeline Builder` class 
                to get the Pipeline Definition
        """
        tasks = (self.spark.read.table(f"{self.metadata_db}.{self.pipeline_tasks_tbl}")
                        .filter(f"current_indicator == 1")
                        .filter(f"pipeline_id == '{pipeline_id}'") ).alias("tsk")
        
        task_list = [(task["task_name"], task) for task in tasks.collect()]
        task_dict = dict(task_list)

        dependencies  = (self.spark.read.table(f"{self.metadata_db}.{self.pipeline_dependencies_tbl}")
                                        .filter(f"pipeline_id == '{pipeline_id}'")
                                        .orderBy("sequence")
                                        .alias("dpn") )
        
        pipeline_dependencies = dependencies.collect()
        
        # print(pipeline_dependencies)

        return self.pipelineBuilder(pipeline_dependencies, task_dict)

    def get_reader_task(self, reader_configs):
        """
            Returns `Reader task` based on the `file format` specified in the `reader options`
        """
        file_format = reader_configs["format"]

        task = PipelineNodeBuilder.GENERIC_BATCH_READER

        reader_tasks = {
                "auto_loader": PipelineNodeBuilder.AUTO_LOADER, 
                "csv" : PipelineNodeBuilder.CSV_READER, 
                "text" : PipelineNodeBuilder.TEXT_READER, 
                "orc" : PipelineNodeBuilder.ORC_READER, 
                "avro" : PipelineNodeBuilder.AVRO_READER, 
                "event_hubs_batch" : PipelineNodeBuilder.EVENT_HUBS_BATCH_READER,
                "event_hubs" : PipelineNodeBuilder.EVENT_HUBS_READER,
                "json" : PipelineNodeBuilder.JSON_READER,
                "kafka": PipelineNodeBuilder.KAFKA_READER,
                "parquet": PipelineNodeBuilder.PARQUET_READER,
                "rdbms" : PipelineNodeBuilder.RDMS_READER
                }
        return reader_tasks.get(file_format, task)

    def get_writer_task(self, writer_configs):
        """
            Returns `Writer task` based on the `format` specified in the `writer options`
        """
        format = writer_configs.get("format", "delta")
        task = PipelineNodeBuilder.DELTA_WRITER
        writer_tasks = {
                        "stream_console": PipelineNodeBuilder.STREAM_CONSOLE_WRITER,
                        "kafka": PipelineNodeBuilder.KAFKA_WRITER
        }
        return writer_tasks.get(format, task)


    def get_processor_task(self, task_configs):
        """
            Returns `Processor task` based on the `processor options`
        """
        task = PipelineNodeBuilder.SQL_PROCESSOR
        if COMMON_CONSTANTS.MASK_COLUMNS in task_configs or COMMON_CONSTANTS.SELECT_COLUMNS in task_configs: 
            task = PipelineNodeBuilder.GENERIC_PROCESSOR
        return task

    def get_task_type(self, task_configs: dict):
        """
            Returns `Task Type` based on the `task configs`
        """
        type = task_configs.get("type")
        if type == "reader":
            return self.get_reader_task(task_configs)
        elif type == "writer":
            return self.get_writer_task(task_configs)
        elif type == "processor":  
            return self.get_processor_task(task_configs)
        return ValueError(f"Incorrect Type: {type}") 

    def get_task(self, task_name: str, task_dict: dict):
        """
            Returns `Pipeline Node` based on the `task metadata`
        """
        task_details = task_dict[task_name]
        task_options = task_details["task_options"]
        task_configs = task_details["task_configs"]
        pipeline_id = task_details["pipeline_id"]
        task_type = self.get_task_type(task_configs)
        task_id = task_details["task_id"]

        Commons.printInfoMessage(f" {task_type}, {task_id}")  
        
        return (PipelineNodeBuilder()
                        .set_task_id(task_id) 
                        .set_name(f"{task_name}") 
                        .set_type(task_type) 
                        .add_input_option("configs", task_configs)
                        .add_input_option("options", task_options)
                        .build() )
    
    def pipelineBuilder(self, pipeline_dependencies: list[Row], task_dict: dict): 
        """
            Builds the `Pipeline` based on the `dependencies` between the `Tasks`
        """
        # Pipeline Info       
        pipeline_id = pipeline_dependencies[0]["pipeline_id"]
        pipeline_name = pipeline_dependencies[0]["pipeline_name"]

        io_service = IOService(self.spark)
        logger = logging.getLogger(__name__)

        builder = PipelineBuilder(self.spark, pipeline_name, logger, pipeline_id, io_service)   
        
        for dpnd in pipeline_dependencies:
            parent_task_name = dpnd["parent"]
            child_task_name = dpnd["child"]
            Commons.printInfoMessage(f"parent_task_name {parent_task_name}, child_task_name {child_task_name}")
            
            parent_tasks = []
            for item in parent_task_name.split(","):
                parent_tasks.append(self.get_task(item.strip(), task_dict) )

            child_tasks = []
            for item in child_task_name.split(","):
                child_tasks.append(self.get_task(item.strip(), task_dict) )
            
            for parent_task in parent_tasks:
                builder = builder.add_node(parent_task)   
                Commons.printInfoMessage(f"parent_task.name ----> `{parent_task.name}` ")
                for child_task in child_tasks:
                    builder = builder.add_node_after(parent_task.name, child_task)
                    Commons.printInfoMessage(f"child_task.name ----> `{child_task.name}` ")
            
        pipeline = builder.build()

        pip_id, name, meta_jsons = PipelineBuilder.get_json_dump(pipeline)
        Commons.printInfoMessage(pip_id)
        Commons.printInfoMessage(name)
        Commons.printInfoMessage(meta_jsons)

        # TODO: Store meta_jsons in `pipeline_options` table
        return pipeline

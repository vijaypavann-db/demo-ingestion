import uuid
 
from com.db.fw.etl.core.pipeline.PipelineBuilder import  *
from pyspark.sql import SparkSession
from pyspark import *
import logging
from com.db.fw.etl.core.common.DeltaStatLogger import IOService


# spark.sql("optimize tata_poc.task_status")
# spark.sql("optimize tata_poc.operational_stats")
# spark.sql("optimize tata_poc.pipeline_status")


class SampleDFPipeline: 

    def __init__(self):
        self.spark = SparkSession.builder.getOrCreate()

    def run(self): 
        reader = PipelineNodeBuilder()\
            .set_name("df_reader")\
            .set_type(PipelineNodeBuilder.SAMPLE_DF_READER)\
            .build()


        processor = PipelineNodeBuilder()\
            .set_name("df_processor")\
            .set_type(PipelineNodeBuilder.SAMPLE_DF_PROCESSOR)\
            .build()

        writer = PipelineNodeBuilder()\
            .set_name("df_writer")\
            .set_type(PipelineNodeBuilder.SAMPLE_DF_WRITER)\
            .build()

        pipeline_name = "demo_graph"
        pipline_id = str(uuid.uuid1())
        io_service =  IOService()
        logger = logging.getLogger(__name__)


        pipeline =  ( PipelineBuilder(self.spark, pipeline_name, logger, pipline_id, io_service)
                        .add_node(reader)
                        # .add_node_after(reader.name,processor)\
                        .add_node_after(processor.name,writer).build()
                    )

        pipline_id,name,meta_jsons = PipelineBuilder.get_json_dump(pipeline)
        print(pipline_id)
        print(name)
        print(meta_jsons)

        pipeline.start()








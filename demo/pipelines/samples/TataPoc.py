import uuid

from demo.TataDigital.TDCustomWriter import ForEachBatchWriter
from demo.TataDigital.TDCustomProcessors import TDBaseEventParseProcessor, MasterProcessor, \
    HistoryWithDQCheckProcessor
from com.db.fw.etl.core.pipeline.PipelineBuilder import *
import logging
from com.db.fw.etl.core.common.Constants import COMMON_CONSTANTS
from com.db.fw.etl.core.common.DeltaStatLogger import IOService
from pyspark.sql import SparkSession

spark = SparkSession.builder.config("spark.jars.packages","com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.21")\
    .config("spark.jars","Downloads/").getOrCreate()

# schema = get_poc_payload_schema(spark)

events_reader = PipelineNodeBuilder() \
    .set_name("event_reader") \
    .set_type(PipelineNodeBuilder.EVENT_HUBS_READER) \
    .add_input_option("YOUR.CONNECTION.STRING",
                      "Endpoint=sb://pvnevntdbns.servicebus.windows.net/;SharedAccessKeyName=DBManageSharedAccessKey;SharedAccessKey=LHtOrPKZR7OWnGDtUJ8krCruazpxw7E5l+EOnBs5/kE=;") \
    .add_input_option("EVENT_HUBS_NAME", "tata_event_hub") \
    .build()

console_writer = PipelineNodeBuilder()\
    .set_name("console_writer")\
    .set_type(PipelineNodeBuilder.STREAM_CONSOLE_WRITER).build()


pipeline_name = "tata_poc_pipeline"
pip_id = str(uuid.uuid1())
io_service = IOService()
logger = logging.getLogger(__name__)

pipeline = PipelineBuilder(spark, pipeline_name, logger, pip_id, io_service) \
    .add_node(events_reader)\
    .add_node_after(events_reader.name,console_writer)\
    .build()


pipeline.start()

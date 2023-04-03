import uuid

from demo.TataDigital.TDCustomWriter import ForEachBatchWriter
from demo.TataDigital.TDCustomProcessors import TDBaseEventParseProcessor, MasterProcessor, \
    HistoryWithDQCheckProcessor
from com.db.fw.etl.core.pipeline.PipelineBuilder import *
import logging
from com.db.fw.etl.core.common.Constants import COMMON_CONSTANTS
from com.db.fw.etl.core.common.DeltaStatLogger import IOService

# spark = SparkSession.builder.getOrCreate()
spark = None
# schema = get_poc_payload_schema(spark)


events_reader = PipelineNodeBuilder() \
    .set_name("event_reader") \
    .set_type(PipelineNodeBuilder.EVENT_HUBS_READER) \
    .add_input_option("YOUR.CONNECTION.STRING",
                      "Endpoint=sb://pvnevntdbns.servicebus.windows.net/;SharedAccessKeyName=DBManageSharedAccessKey;SharedAccessKey=LHtOrPKZR7OWnGDtUJ8krCruazpxw7E5l+EOnBs5/kE=;") \
    .add_input_option("EVENT_HUBS_NAME", "tata_event_hub") \
    .build()

base_event_parse_processor = PipelineNodeBuilder.build_custom_node(
    TDBaseEventParseProcessor("base_event_parse_processor",
                              PipelineNodeBuilder.CUSTOM_PROCESSOR))

master_table_tcp_payment_processor = PipelineNodeBuilder.build_custom_node(
    MasterProcessor("master_table_tcp_payment_processor",
                    PipelineNodeBuilder.CUSTOM_PROCESSOR))

history_with_dq_check_processor = PipelineNodeBuilder.build_custom_node(
    HistoryWithDQCheckProcessor("history_with_dq_check_processor",
                                PipelineNodeBuilder.CUSTOM_PROCESSOR))

master_table_delta_writer = PipelineNodeBuilder() \
    .set_name("master_table_delta_writer") \
    .set_type(PipelineNodeBuilder.DELTA_WRITER)\
    .add_input_option("db_name","tata_poc")\
    .add_input_option("table_name","master_table")\
    .build()

master_table_tcp_payment_writer = PipelineNodeBuilder() \
    .set_name("master_table_tastcp_payment_writer") \
    .set_type(PipelineNodeBuilder.DELTA_WRITER) \
    .add_input_option("mode", "append") \
    .add_input_option(COMMON_CONSTANTS.DB_NAME, "tata_poc") \
    .add_input_option(COMMON_CONSTANTS.TABLE_NAME, "master_table_tcp_payment") \
    .add_input_option(COMMON_CONSTANTS.CHECK_POINT_LOCATION, "/tmp/tata_poc/check_points/master") \
    .add_input_option(COMMON_CONSTANTS.TRIGGER_TIME, "1 seconds") \
    .build()

forEachBatchWriterTask = ForEachBatchWriter("foreach_writer_for_multi_location_writer",
                                            PipelineNodeBuilder.CUSTOM_PROCESSOR)

forEachBatchWriterTask.add_option_value("checkpointLocation", "/tmp/tata_poc/check_points/foreach")
forEachBatchWriterTask.add_option_value("part_count", "")
forEachBatchWriterTask.add_option_value("postgres_host", "yatin-tatadigital-demo.postgres.database.azure.com")
forEachBatchWriterTask.add_option_value("postgres_user", "yatin_kumar@yatin-tatadigital-demo")
forEachBatchWriterTask.add_option_value("postgres_pwd", "tata123#")
forEachBatchWriterTask.add_option_value("postgres_database", "tdpoc")
forEachBatchWriterTask.add_option_value("history_db_name", "tata_poc")
forEachBatchWriterTask.add_option_value("history_table_name", "history")
forEachBatchWriterTask.add_option_value("fact_db_name", "tata_poc")
forEachBatchWriterTask.add_option_value("fact_table_name", "fact_table")
forEachBatchWriterTask.add_option_value("merge_condition", "fact.customer_hash = src.customer_hash AND fact.source_order_detail_creation_date = src.source_order_detail_creation_date")
forEachBatchWriterTask.add_option_value("do_update", True)
forEachBatchWriterTask.add_option_value("do_insert", True)
forEachBatchWriterTask.add_option_value("update_condition", "fact.source_order_detail_updation_date < src.source_order_detail_updation_date")



















foreach_writer_for_multi_location_writer = PipelineNodeBuilder.build_custom_node(forEachBatchWriterTask)

pipeline_name = "tata_poc_pipeline"
pip_id = str(uuid.uuid1())
io_service = IOService()
logger = logging.getLogger(__name__)

pipeline = PipelineBuilder(spark, pipeline_name, logger, pip_id, io_service) \
    .add_node(events_reader) \
    .add_node_after(events_reader.name, base_event_parse_processor) \
    .add_node_after(base_event_parse_processor.name, master_table_tcp_payment_processor) \
    .add_node_after(master_table_tcp_payment_processor.name, master_table_tcp_payment_writer) \
    .add_node_after(base_event_parse_processor.name, history_with_dq_check_processor) \
    .add_node_after(history_with_dq_check_processor.name, foreach_writer_for_multi_location_writer).build()

# pipeline.start()

import datetime
import json
import time

from demo.TataDigital.TDUtilities import PostgresWriterService
from com.db.fw.etl.core.common.Commons import Commons
from com.db.fw.etl.core.common.DeltaStatLogger import IOService
from com.db.fw.etl.core.writers.CommonWriters import BaseWriter
from pyspark.sql.functions import *
from pyspark.sql import *
from com.db.fw.etl.core.writers.CommonWritingUtils import *
from com.db.fw.etl.core.common.Constants import COMMON_CONSTANTS
import psycopg2


#
# # This class will be replced by ForEachBatchWriterNotebook
# class ForEachBatchWriter(BaseWriter):
#
#     def get_latest_snapshot_rec(self,df,batch_id):
#         df.createOrReplaceGlobalTempView("factView_{}".format(batch_id))
#         latest_snaphot_df = self.spark.sql(
#             "SELECT *, row_number() over (partition by customer_hash order by source_order_detail_updation_date desc) row_number,"
#             "CASE WHEN (id % 2) = 0 THEN 'loyal' ELSE 'non-loyal' END customer_type FROM factView_${batchId} WHERE member_id_present = true AND cust_ hash_present = true ".format(batch_id))\
#             .where("row_number = 1")\
#             .drop("row_number")
#         return latest_snaphot_df
#
#
#     def write_to_history(self, df ):
#         insert_options = {}
#         insert_options[COMMON_CONSTANTS.DB_NAME] = self.get_option_value("history_db_name")
#         insert_options[COMMON_CONSTANTS.TABLE_NAME] = self.get_option_value("history_table_name")
#
#         delta_insert(df,insert_options,COMMON_CONSTANTS.APPEND)
#
#     def merge_to_fact(self,df,batch_id):
#         merge_options = {}
#         merge_options[COMMON_CONSTANTS.DB_NAME] = self.get_option_value("fact_db_name")
#         merge_options[COMMON_CONSTANTS.TABLE_NAME] = self.get_option_value("fact_table_name")
#         merge_options[COMMON_CONSTANTS.DO_UPDATE] = "true"
#         merge_options[COMMON_CONSTANTS.DO_INSERT] = "true"
#         merge_options[COMMON_CONSTANTS.MERGE_CONDITION] = " target.customer_hash = source.customer_hash AND target.source_order_detail_creation_date = source.source_order_detail_creation_date "
#         merge_options[COMMON_CONSTANTS.UPDATE_CONDITION] = "target.source_order_detail_updation_date < source.source_order_detail_updation_date"
#         delta_merge(df,merge_options)
#
#
#
#     def multi_location_write(self,df, batch_id):
#
#
#         # Run below code in thread
#
#         # Write to History
#         self.writeToHistory(df)
#
#         #write to Exception Table
#
#
#         # Write to History
#         latest_snapshot_df = self.getLatestSnapshotRec(df,batch_id)
#         self.merge_to_fact(latest_snapshot_df,batch_id)
#
#         # Write to Postgres
#         self.upsert_to_postgres(latest_snapshot_df)
#
#     def execute(self):
#         stream_input_df = self.get_input_dataframe();
#         param_json = json.dumps(self.input_options);
#
#         stream_input_df = stream_input_df.withColumn("param",lit(param_json))
#         check_point = self.get_option_value("checkpointLocation")
#
#         stream_input_df.writeStream\
#             .foreachBatch(self.multi_location_write)\
#             .outputMode("update") \
#             .option("checkpointLocation", check_point)\
#             .start()
#

# class ForEachBatchWriterNotebook(BaseWriter):
#
#     def get_latest_snapshot_rec(self, df, batch_id):
#         df.createOrReplaceTempView("factView".format(batch_id))
#         latest_snaphot_df = self.spark.sql(
#             f"""SELECT*,row_number() over (partition by customer_hash order by source_order_detail_updation_date desc) row_number, CASE WHEN (payOrderId % 2) = 0 THEN 'loyal' ELSE 'non-loyal' END customer_type FROM factView WHERE member_id_present = true AND cust_hash_present = true """).where(
#             "row_number = 1").drop("row_number")
#
#         return latest_snaphot_df
#
#     def multi_location_write(self, df, batch_id):
#        pass
#
#     def execute(self):
#         history_df = self.get_input_dataframe();
#
#         check_point = self.get_option_value("checkpointLocation")
#
#
#         batch_id = None
#
#         part_count = None,
#         postgres_host = None,
#         postgres_user = None,
#         postgres_pwd = None,
#         postgres_database = None
#
#         #         history_df = history_df.distinct()
#
#         #         history_df = history_df.limit(1000)
#         print("History count {}".format(history_df.count()))
#         pgWriter = PostgresWriterService()
#         pgWriter.upsert(history_df)
#
#             batch_id = self.input_options.get("batch_id",None)
#             part_count = self.input_options.get("part_count",None)
#             postgres_host = self.input_options.get("postgres_host",None)
#             postgres_user = self.input_options.get("postgres_user",None)
#             postgres_pwd = self.input_options.get("postgres_pwd",None)
#             postgres_database = self.input_options.get("postgres_database",None)

#             history_db_name = self.input_options.get("history_db_name",None)
#             history_table_name = self.input_options.get("history_table_name",None)

#             fact_db_name = self.input_options.get("fact_db_name",None)
#             fact_db_name = self.input_options.get("fact_db_name",None)
#             fact_table_name = self.input_options.get("fact_table_name",None)
#             merge_condition = self.input_options.get("merge_condition",None)
#             do_update = self.input_options.get("do_update",None)
#             do_insert  = self.input_options.get("do_insert",None)
#             update_condition  = self.input_options.get("update_condition",None)

#             total_input_rows_for_processing = None
#             self.add_facts("input_rows" ,total_input_rows_for_processing )

#             total_row_for_cs_upsert_postgress= None
#             self.add_facts("cs_upsert" ,total_input_rows_for_processing )

#             total_row_for_fact_upsert_delta= None
#             self.add_facts("fact_upsert" ,total_row_for_fact_upsert_delta )

#             loyal_cust_count = None
#             self.add_facts("loyal_cust_count" ,total_row_for_fact_upsert_delta )

#             non_loyal_count = None
#             self.add_facts("non_loyal_count" ,total_row_for_fact_upsert_delta )

#             member_dim_present_count= None
#             self.add_facts("member_dim_present_count" ,total_row_for_fact_upsert_delta )

#             cust_dim_present_count= None
#             self.add_facts("member_dim_present_count" ,cust_dim_present_count )

#             member_and_cust_dim_present_count= None
#             self.add_facts("member_and_cust_dim_present_count" ,cust_dim_present_count )

#             self.write_to_history(history_df,self.input_options)
#             print("history_df count {}".format(history_df.count()))

#             latest_snapshot_df = self.get_latest_snapshot_rec(history_df, 1)
#             print("latest_snapshot_df count {}".format(latest_snapshot_df.count()))

#             self.merge_to_fact(latest_snapshot_df,self.input_options)

#             pgWriter = PostgresWriterService()
#             pgWriter.upsert(latest_snapshot_df)

#
#         self.set_output_dataframe(history_df)

class ForEachBatchWriter_test(BaseWriter):

    def get_latest_snapshot_rec(self, df, batch_id):
        df.createOrReplaceGlobalTempView("factView_{}".format(batch_id))
        latest_snaphot_df = self.spark.sql(
            f"""SELECT*,row_number() over (partition by customer_hash order by source_order_detail_updation_date desc) row_number, CASE WHEN (payOrderId % 2) = 0 THEN 'loyal' ELSE 'non-loyal' END customer_type, mod(abs(hash(customer_hash)),1000) customer_bucket FROM global_temp.factView_{batch_id} WHERE member_id_present = true AND cust_hash_present = true """).where(
            "row_number = 1").drop("row_number")

        return latest_snaphot_df

    def multi_location_write(self, history_df, batch_id):
        history_df.persist()
        if len(history_df.head(1)) > 0 :
            params = history_df.select("param").first()[0]
            print("from multi_location_write params : {}".format(str(params)))

            input_options = json.loads(params)

            print("from multi_location_write input_options : {}".format(str(input_options)))


            batch_id = int(time.time() * 1000)

            history_df = history_df.drop("param")
            self.write_to_history(history_df, input_options, batch_id)
            print("history_df count {}".format(history_df.count()))

            latest_snapshot_df = self.get_latest_snapshot_rec(history_df, batch_id)
            print("latest_snapshot_df count {}".format(latest_snapshot_df.count()))

            self.merge_to_fact(latest_snapshot_df, input_options, batch_id)

            pgWriter = PostgresWriterService()
            pgWriter.upsert(latest_snapshot_df)
            self.set_output_dataframe(latest_snapshot_df)

            self.add_all_operational_stats(history_df, latest_snapshot_df,batch_id)

        history_df.unpersist()

    def add_all_operational_stats(self, history_df, latest_snapshot_df, batch_id):
        io_service = IOService()
        status_time = Commons.get_curreny_time()
        facts = {}
        total_input_rows_for_processing = history_df.count()
        facts["input_rows"] = total_input_rows_for_processing
        total_row_for_cs_upsert_postgress = latest_snapshot_df.count()
        facts["cs_upsert"] = total_row_for_cs_upsert_postgress
        total_row_for_fact_upsert_delta = latest_snapshot_df.count()
        facts["fact_upsert"] = total_row_for_fact_upsert_delta
        loyal_cust_count = latest_snapshot_df.filter("customer_type=='loyal'").count()
        facts["loyal_cust_count"] = loyal_cust_count
        non_loyal_count = latest_snapshot_df.filter("customer_type=='non-loyal'").count()
        facts["non_loyal_count"] = non_loyal_count
        member_dim_present_count = history_df.filter("member_id_present=='true'").count()
        facts["member_dim_present_count"] = member_dim_present_count
        cust_dim_present_count = history_df.filter("cust_hash_present=='true'").count()
        facts["member_dim_present_count"] = cust_dim_present_count
        member_and_cust_dim_present_count = history_df.filter("cust_hash_and_member_id_present=='true'").count()
        facts["member_and_cust_dim_present_count"] = member_and_cust_dim_present_count
        io_service.store_operational_stats(self.pipeline_uid,
                                           self.pipeline_name,
                                           self.task_name,
                                           facts, status_time, batch_id)


    def add_all_operational_stats_old(self, history_df, latest_snapshot_df,batch_id):
        io_service = IOService()
        status_time = Commons.get_curreny_time()

        facts = {}
        total_input_rows_for_processing = 0
        facts["input_rows"] = total_input_rows_for_processing

        total_row_for_cs_upsert_postgress = 0
        facts["cs_upsert"] = total_row_for_cs_upsert_postgress

        total_row_for_fact_upsert_delta = 0
        facts["fact_upsert"] = total_row_for_fact_upsert_delta

        loyal_cust_count = 0
        facts["loyal_cust_count"] = loyal_cust_count

        non_loyal_count = 0
        facts["non_loyal_count"] = non_loyal_count

        member_dim_present_count = 0
        facts["member_dim_present_count"] = member_dim_present_count

        cust_dim_present_count = 0
        facts["member_dim_present_count"] = cust_dim_present_count

        member_and_cust_dim_present_count = 0
        facts["member_and_cust_dim_present_count"] = member_and_cust_dim_present_count

        io_service.store_operational_stats(self.pipeline_uid,
                                           self.pipeline_name,
                                           self.task_name,
                                           facts, status_time,batch_id)

        pass

    def write_to_history(self, df, input_options, batch_id):
        # start work on this
        db_name = input_options.get("history_db_name",None)
        table_name = input_options.get("history_table_name",None)

        input_options[COMMON_CONSTANTS.DB_NAME] =input_options.get("history_db_name",None)
        input_options[COMMON_CONSTANTS.TABLE_NAME] = input_options.get("history_table_name",None)

        if batch_id % 10 == 0:
            Commons.printInfoMessage( "for write_to_history db name {} table name {} ".format(db_name,table_name) )
            if db_name is not None and table_name is not None:
                self.spark.sql("OPTIMIZE {}.{}".format(db_name, table_name))

        # if (batchId % 101 == 0){spark.sql("optimize events zorder by (eventType)")}
        delta_insert(self.spark, df, input_options, COMMON_CONSTANTS.APPEND)

    def merge_to_fact(self, df, input_options, batch_id):
        # start work on this
        db_name = input_options.get("fact_db_name", None)
        table_name = input_options.get("fact_table_name", None)

        if batch_id % 10 == 0:
            if db_name is not None and table_name is not None:
                self.spark.sql("OPTIMIZE {}.{}".format(db_name, table_name))

        delta_merge(self.spark, df, input_options)

    def execute(self):
        history_df = self.get_input_dataframe();

        check_point = self.input_options.get("checkpointLocation")

        json_params = json.dumps(self.input_options)
        print("********** {}".format(str(json_params)))

        history_df = history_df.withColumn("param", lit(json_params))

        tataFactStreamingQuery = history_df.writeStream.option("checkpointLocation", check_point) \
            .queryName("Tata Fact Pipeline Events") \
            .outputMode("append") \
            .trigger(processingTime='60 seconds') \
            .foreachBatch(self.multi_location_write) \
            .start()

        self.set_output_dataframe(history_df)

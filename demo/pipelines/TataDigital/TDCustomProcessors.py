from pyspark.sql.functions import from_json, explode,size

from demo.TataDigital.TDUtilities import get_poc_payload_schema
from com.db.fw.etl.core.common.Task import Task
from com.db.fw.etl.core.processor.CommonProcessors import BaseProcessor


class TDBaseEventParseProcessorNotebook(BaseProcessor):
    def __init__(self, name, type):
        Task.__init__(self, name, type)

    def execute(self):
            schema =self.spark.read.option("multiLine", True).json("/FileStore/exportv1.json").schema
            df = self.get_input_dataframe()
            df = df.selectExpr("cast(body AS String) body", "enqueuedTime")
            df = df.withColumn("payload", from_json(df["body"], schema))
            df = df.select("payload.*", "body", "enqueuedTime")
            df = df.withColumn("dataSize", size(df["data"]))
            payloadDf = df.filter("dataSize > 0")
            payloadDf = payloadDf.withColumn("data", explode(payloadDf["data"]))
            self.set_output_dataframe(payloadDf )



class MasterProcessorNotebook(BaseProcessor):
    def __init__(self, name, type):
        Task.__init__(self, name, type)

    def execute(self):
        payloadDf = self.get_input_dataframe()

        payloadMasterDf = payloadDf.selectExpr("event_type", "cast(data AS String) AS data",
                                                     """map('application', metadata.application,
                                                         'operation', metadata.operation,
                                                         'tableName', metadata.tableName,
                                                         'timeStamp', metadata.timeStamp,
                                                         'version', metadata.version) AS metadata""",
                                                     "body AS json", "enqueuedTime AS loaddate",
                                                     "current_date AS partition_date")

        self.set_output_dataframe(payloadMasterDf)


class HistoryWithDQCheckProcessorNotebook(Task):
    def __init__(self, name, type):
        Task.__init__(self, name, type)

    def execute(self):
        payloadParsedDf = self.get_input_dataframe()


        payloadParsedDf = payloadParsedDf.selectExpr("data.merchantOrderId AS source_order_detail_id",
                                                     "data.merchantOrderId AS source_order_header_id",
                                                     "data.custId AS customer_hash",
                                                     "CAST(data.merchantId AS STRING) AS source_member_id",
                                                     "CAST(0 AS LONG) AS source_member_address_id",
                                                     "CAST(0 AS LONG) AS source_city_id",
                                                     "CAST(0 AS LONG) AS source_hub_id",
                                                     "CAST(0 AS LONG) AS source_slot_id",
                                                     "CAST(data.merchantOrderSkuId AS LONG) AS source_sku_id",
                                                     "CAST(data.vendorId AS LONG) AS supplier_id",
                                                     "to_timestamp(data.creationDateTime) AS source_order_creation_date",
                                                     "current_date() AS order_delivery_date",
                                                     "CAST(data.payOrderId AS STRING) AS order_number",
                                                     "data.orderStatus AS order_status",
                                                     "'dummy_chnl' AS order_channel",
                                                     "'dummy_fl' AS fulfillment_type",
                                                     "CAST(data.skuQuantity AS DECIMAL(11,2)) AS quantity",
                                                     "CAST(data.itemPayableAmount AS DECIMAL(11,2)) AS unit_mrp",
                                                     "CAST(data.itemPayableAmount AS DECIMAL(11,2)) AS unit_sale_price",
                                                     "CAST(data.itemTotalAmount AS DECIMAL(11,2)) AS total_item_price",
                                                     "to_timestamp(data.creationDateTime) AS source_order_detail_creation_date",
                                                     "to_timestamp(data.lastUpdatedDateTime) AS source_order_detail_updation_date",
                                                     "'dummy_ot' AS order_type",
                                                     "CAST(0 AS LONG) AS pick_location_id",
                                                     "CAST(0 AS DECIMAL(11, 2)) AS item_delivery_charge",
                                                     "'dummy_ds' AS data_source",
                                                     "CAST(0 AS LONG) AS source_sa_id",
                                                     "CAST(0 AS LONG) AS source_delivery_mode_id",
                                                     "enqueuedTime AS loaddate",
                                                     "current_date AS partition_date",
                                                     "current_timestamp AS added_date",
                                                     "current_timestamp AS updated_date",
                                                     "data.payOrderId")

        payloadDfPart = payloadParsedDf.repartition(payloadParsedDf["customer_hash"])

        payloadDfPart.createOrReplaceTempView("payloadHistoryView")

        customerTable = "tata_poc.master_cust_ids"
        memberTable = "tata_poc.customer_member_dim_fact"

        historyWithDimLookUpDF = self.spark.sql(
            f"""
        select /*+ BROADCAST({customerTable}), BROADCAST({memberTable}) */ 
          hist.*, 
              CASE WHEN
                  memb.member_hash IS NOT null THEN true
                  ELSE false
                END member_id_present,
                CASE WHEN
                  cust.cust_hash IS NOT null THEN true
                  ELSE false
                END cust_hash_present,
                CASE WHEN
                  cust.cust_hash IS NOT null AND memb.member_hash IS NOT null THEN true
                  ELSE false
                END cust_hash_and_member_id_present
             FROM payloadHistoryView hist 
            LEFT OUTER JOIN {customerTable} cust ON (hist.customer_hash = cust.cust_hash)
            LEFT OUTER JOIN {memberTable} memb ON (hist.source_member_id = memb.member_hash)
        """.format(customerTable, memberTable))

        self.set_output_dataframe(historyWithDimLookUpDF)

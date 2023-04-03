from pyspark.sql import SparkSession

def get_poc_payload_schema(spark):
    df = spark.read.option("multiLine", True).json("dbfs:/tmp/td.json")
    return df.schema


import psycopg2


class PostgresWriterService:
    def prep_upsert_query(self, row):
        val_1 = row.__getitem__("source_order_detail_id")
        val_2 = row.__getitem__("source_order_header_id")
        val_3 = row.__getitem__("customer_hash")
        val_4 = row.__getitem__("source_member_id")
        val_5 = row.__getitem__("source_member_address_id")
        val_6 = row.__getitem__("source_city_id")
        val_7 = row.__getitem__("source_hub_id")
        val_8 = row.__getitem__("source_slot_id")
        val_9 = row.__getitem__("source_sku_id")
        val_10 = row.__getitem__("supplier_id")
        val_11 = row.__getitem__("source_order_creation_date")
        val_12 = row.__getitem__("order_delivery_date")
        val_13 = row.__getitem__("order_number")
        val_14 = row.__getitem__("order_status")
        val_15 = row.__getitem__("order_channel")
        val_16 = row.__getitem__("fulfillment_type")
        val_17 = row.__getitem__("quantity")
        val_18 = row.__getitem__("unit_mrp")
        val_19 = row.__getitem__("unit_sale_price")
        val_20 = row.__getitem__("total_item_price")
        val_21 = row.__getitem__("source_order_detail_creation_date")
        val_22 = row.__getitem__("source_order_detail_updation_date")
        val_23 = row.__getitem__("order_type")
        val_24 = row.__getitem__("pick_location_id")
        val_25 = row.__getitem__("item_delivery_charge")
        val_26 = row.__getitem__("data_source")
        val_27 = row.__getitem__("source_sa_id")
        val_28 = row.__getitem__("source_delivery_mode_id")
        val_29 = row.__getitem__("loaddate")
        val_30 = row.__getitem__("partition_date")
        val_31 = row.__getitem__("added_date")
        val_32 = row.__getitem__("updated_date")
        val_33 = row.__getitem__("customer_type")

        return (val_1, val_2, val_3, val_4, val_5, val_6, val_7, val_8, val_9, val_10, val_11, val_12, val_13, val_14, val_15,
    val_16, val_17, val_18, val_19, val_20, val_21, val_22, val_23, val_24, val_25, val_26, val_27, val_28, val_29,
    val_30, val_31, val_32, val_33)

    def process_partition(self, partition):
        conn = psycopg2.connect(host="yatin-tatadigital-demo.postgres.database.azure.com",
                                user="yatin_kumar@yatin-tatadigital-demo", password="tata123#", database="tdpoc")
        dbc_merge = conn.cursor()
        sql_string = f"""INSERT INTO fact_table_tcp_payment (source_order_detail_id,source_order_header_id,customer_hash,source_member_id,source_member_address_id,source_city_id,source_hub_id,source_slot_id,source_sku_id,supplier_id,source_order_creation_date,order_delivery_date,order_number,order_status,order_channel,fulfillment_type,quantity,unit_mrp,unit_sale_price,total_item_price,source_order_detail_creation_date,source_order_detail_updation_date,order_type,pick_location_id,item_delivery_charge,data_source,source_sa_id,source_delivery_mode_id,loaddate,partition_date ,added_date,updated_date,customer_type)
  VALUES ('%s', '%s',%s, %s, '%s','%s', '%s', '%s', '%s','%s', %s, %s,%s,%s,%s,%s, '%s', '%s','%s', '%s', %s,%s,%s, '%s','%s',%s, '%s','%s',%s,%s,%s,%s,%s)
  ON CONFLICT (customer_hash,source_order_detail_creation_date)
  DO
  UPDATE SET source_order_header_id=EXCLUDED.source_order_header_id,customer_hash=EXCLUDED.customer_hash,source_member_id=EXCLUDED.source_member_id,source_member_address_id=EXCLUDED.source_member_address_id,source_city_id=EXCLUDED.source_city_id,source_hub_id=EXCLUDED.source_hub_id,source_slot_id=EXCLUDED.source_slot_id,source_sku_id=EXCLUDED.source_sku_id,supplier_id=EXCLUDED.supplier_id,source_order_creation_date=EXCLUDED.source_order_creation_date,order_delivery_date=EXCLUDED.order_delivery_date,order_number=EXCLUDED.order_number,order_status=EXCLUDED.order_status,order_channel=EXCLUDED.order_channel,fulfillment_type=EXCLUDED.fulfillment_type,quantity=EXCLUDED.quantity,unit_mrp=EXCLUDED.unit_mrp,unit_sale_price=EXCLUDED.unit_sale_price,total_item_price=EXCLUDED.total_item_price,source_order_detail_creation_date=EXCLUDED.source_order_detail_creation_date,source_order_detail_updation_date=EXCLUDED.source_order_detail_updation_date,order_type=EXCLUDED.order_type,pick_location_id=EXCLUDED.pick_location_id,item_delivery_charge=EXCLUDED.item_delivery_charge,data_source=EXCLUDED.data_source,source_sa_id=EXCLUDED.source_sa_id,source_delivery_mode_id=EXCLUDED.source_delivery_mode_id,loaddate=EXCLUDED.loaddate,partition_date=EXCLUDED.partition_date,added_date=EXCLUDED.added_date,updated_date=EXCLUDED.updated_date,customer_type=EXCLUDED.customer_type"""

        vars_list = []
        for row in partition:
            value_tuple = self.prep_upsert_query(row)
        vars_list.append(value_tuple)

        dbc_merge.executemany(sql_string, vars_list)

        conn.commit()
        dbc_merge.close()
        conn.close()

    def upsert(self, df):
        df = df.na.fill(0)
        df.foreachPartition(self.process_partition)
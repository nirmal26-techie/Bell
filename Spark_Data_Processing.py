from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import col, lit, lead, when
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from pyspark.sql.functions import col, lit, max as max_, to_date
from pyspark.sql.functions import max as spark_max


conf = SparkConf().setAppName("pyspark").getOrCreate()
sc = SparkContext(conf=conf)
spark = SparkSession.builder \
    .appName("DeltaApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()


df_customers = spark.read.option("header","true").csv("gs://bell_raw_ingestion_layer/Customers_Raw_Data/")  # reading from gcs bucket raw customers file
w = Window.partitionBy("customer_id").orderBy("change_date")
#Implementing logic to compute valid_from and valid_to so as SCD Type 2 can be implemented
df_customers = df_customers.withColumn("valid_from", col("change_date")) \
    .withColumn("valid_to", lead("change_date").over(w)) \
    .withColumn("is_current", lead("change_date").over(w).isNull()) \
    .withColumn("is_current", when(col("is_current") == True, "Yes").otherwise("No")) \
    .drop("change_date")

# Data Quality Checks for the Customers Table
df_customers = df_customers.filter(col("customer_id").isNotNull())\
       .filter(col("email").contains("@"))\
       .filter(length(trim(col("phone_number"))) >= 10)\
       .filter(length(col("zip_code")) >= 4)


import subprocess
path_exists_customer =subprocess.run(["gsutil","ls","gs://processedlayersilver/customers_proce/"],stdout=subprocess.PIPE,stderr=subprocess.PIPE)

if not path_exists_customer:

    df_customers.write.format("delta").save("gs://processedlayersilver/customers_proce/")                                   # if path not exists it will be a first write

else:

    delta_table_customers = DeltaTable.forPath(spark,"gs://processedlayersilver/customers_proce/")                        # I am gonna write in destination as a delta format(to take care of incrmental loading,schema evolution,updates)
    #Update old records in Delta (set is_current = No, set valid_to)
    delta_table_customers.alias("target").merge(
        source=df_cusomers.alias("source"),
        condition="target.customer_id = source.customer_id AND target.is_current = 'Yes'"
    ).whenMatchedUpdate(set={
        "valid_to": col("source.valid_from"),
        "is_current": lit("No")
    }).execute()

# Insert new records (updated or new customers)
delta_table.alias("target").merge(
    source=df_cusomers.alias("source"),
    condition="target.customer_id = source.customer_id AND target.valid_from = source.valid_from"
).whenNotMatchedInsertAll().execute()

# # Billing data processing start

df_billing = spark.read.option("header", True).csv("gs://bell_raw_ingestion_layer/billing_raw_data/")         #reading data raw from gcs bucket(please note I am comfortable with other cloud providers as well)

#ensuring everything is in right format/datatype

df_billing = df_billing.withColumn("billing_month", to_date("billing_month")) \
       .withColumn("payment_date", to_date("payment_date")) \
       .withColumn("amount_due", col("amount_due").cast("double")) \
       .withColumn("amount_paid", col("amount_paid").cast("double"))

path_exists_billing =subprocess.run(["gsutil","ls","gs://processedlayersilver/billing_processed/"],stdout=subprocess.PIPE,stderr=subprocess.PIPE)

if not path_exists_billing:
    df_billing.write.format("delta").save("gs://processedlayersilver/billing_processed/")                                # writing for the first time

else:
    if DeltaTable.isDeltaTable(spark,"gs://processedlayersilver/billing_processed/" ):
        delta_table = DeltaTable.forPath(spark,"gs://processedlayersilver/billing_processed/")

        merge_condition = "existing.bill_id = updates.bill_id AND existing.payment_date = updates.payment_date"

        (
            delta_table.alias("existing")
            .merge(
                source=df_billing.alias("updates"),
                condition=merge_condition
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

#usage data processing

df_usage = spark.read.option("header", True).csv("gs://bell_raw_ingestion_layer/usage_raw_data")

df_usage = df_usage.withColumn("usage_date", to_date("usage_date")) \
       .withColumn("call_minutes", col("call_minutes").cast("double")) \
       .withColumn("data_usage_gb", col("data_usage_gb").cast("double")) \
       .withColumn("sms_count", col("sms_count").cast("int"))

df_usage = df_usage.withColumn("total_usage_score",
                   col("call_minutes") * 0.4 +
                   col("data_usage_gb") * 0.5 +
                   col("sms_count") * 0.1)

df_usage = df_usage.withColumn("usage_category", when(col("total_usage_score") > 500, "High")
                                      .when(col("total_usage_score") > 200, "Medium")
                                      .otherwise("Low"))

df_usage = df_usage.withColumn("is_weekend", when(dayofweek("usage_date").isin([1, 7]), True).otherwise(False))

path_exists_usage =subprocess.run(["gsutil","ls","gs://processedlayersilver/usage_processed/"],stdout=subprocess.PIPE,stderr=subprocess.PIPE)

if not path_exists_usage:
    df_usage.write.format("delta").save("gs://processedlayersilver/usage_processed/")                                    # if path doesn exists its gonna be the first write

else:
    delta_table2 = DeltaTable.forPath(spark, "gs://processedlayersilver/usage_processed/")

    merge_condition = "existing_usage.usage_id = updates_usage.usage_id AND existing_usage.usage_date = updates_usage.usage_date"

    (
        delta_table2.alias("existing_usage")
        .merge(
            source=df_usage.alias("updates_usage"),
            condition=merge_condition
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )                                                                                                                    #after all the data is stored in processed layer we will analuze it using databricks















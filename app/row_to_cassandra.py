from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_json, struct, date_trunc, to_timestamp
from pyspark.sql.types import StructType, StringType, BooleanType
import time

spark = SparkSession.builder \
    .appName("WriteToCassandra") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

kafka_server = "kafka-server:9092"
input_topic = "wiki-topic"
cassandra_keyspace = "wiki_keyspace"
cassandra_table = "wiki_row"


# reading
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_server) \
    .option("subscribe", input_topic) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .option("maxOffsetsPerTrigger", 1000) \
    .load()

schema = StructType() \
    .add("meta", StructType().add("domain", StringType()).add("id", StringType())) \
    .add("page_id", StringType()) \
    .add("page_title", StringType()) \
    .add("performer", StructType() \
        .add("user_id", StringType()) \
        .add("user_text", StringType()) \
        .add("user_is_bot", BooleanType())) \
    .add("rev_timestamp", StringType())

# parsing json
json_df = df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select(
        col("data.meta.domain").alias("domain"),
        col("data.meta.id").alias("event_id"),
        col("data.page_id").alias("page_id"),
        col("data.page_title").alias("title"),
        col("data.performer.user_is_bot").alias("is_bot"),
        col("data.performer.user_id").alias("user_id"),
        col("data.performer.user_text").alias("user_name"),
        to_timestamp(col("data.rev_timestamp")).alias("timestamp")
    ) \
    .withColumn("event_hour", date_trunc("hour", col("timestamp"))) \
    .filter(col("user_id").isNotNull())

 
# filtered_df = json_df.filter((col("user_id").isNotNull()))

def process_batch(batch_df, batch_id):
    count = batch_df.count()
    if count > 0:
        print(f"Processing batch: {batch_id}; amount: {batch_df.count()}")
        try:
            batch_df.write \
                .format("org.apache.spark.sql.cassandra") \
                .options(table=cassandra_table, keyspace=cassandra_keyspace) \
                .mode("append") \
                .save()
            print(f"Batch {batch_id} written to Cassandra")
        except Exception as e:
            print(f"Error writing batch {batch_id} to Cassandra: {e}")
    else:
        print(f"Batch {batch_id} has no valid records, skipping")

# writing
query = json_df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .option("checkpointLocation", "/opt/app/cassandra_writer_checkpoint") \
    .trigger(processingTime="10 seconds") \
    .start()

query.awaitTermination()
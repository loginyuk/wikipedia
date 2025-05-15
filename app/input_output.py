# stream_filter.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_json, struct
from pyspark.sql.types import StructType, StringType, BooleanType

spark = SparkSession.builder \
    .appName("FilterWikipediaEvents") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

kafka_server = "kafka-server:9092"
input_topic = "input"
output_topic = "processed"

# define schema for json
schema = StructType() \
    .add("meta", StructType().add("domain", StringType()).add("id", StringType())) \
    .add("performer", StructType()
        .add("user_id", StringType())
        .add("user_is_bot", BooleanType())) \
    .add("rev_timestamp", StringType()) \
    .add("page_title", StringType())


# reading
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_server) \
    .option("subscribe", input_topic) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .option("maxOffsetsPerTrigger", 1000) \
    .load()

# parsing json
json_df = df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select(
        col("data.meta.domain").alias("domain"),
        col("data.meta.id").alias("event_id"),
        col("data.performer.user_is_bot").alias("user_is_bot"),
        col("data.performer.user_id").alias("user_id"),
        col("data.rev_timestamp").alias("created_at"),
        col("data.page_title").alias("page_title")
    )

# filter data
filtered_df = json_df.filter(
    (col("domain").isin("en.wikipedia.org", "www.wikidata.org", "commons.wikimedia.org")) &
    (col("user_is_bot") == False) &
    (col("user_id").isNotNull())
)

# writing
query = filtered_df \
    .select(to_json(struct("*")).alias("value")) \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_server) \
    .option("topic", output_topic) \
    .option("checkpointLocation", "/opt/app/stream_filter_checkpoint") \
    .trigger(processingTime="10 seconds") \
    .outputMode("append") \
    .start()

query.awaitTermination()
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType
import time

spark = SparkSession.builder \
    .appName("WriteToCassandra") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

kafka_server = "kafka-server:9092"
output_topic = "processed"
cassandra_keyspace = "hw12_lohin"
cassandra_table = "wiki"


# reading
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_server) \
    .option("subscribe", output_topic) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .option("maxOffsetsPerTrigger", 1000) \
    .load()

# define schema for json
schema = StructType() \
    .add("user_id", StringType()) \
    .add("domain", StringType()) \
    .add("created_at", StringType()) \
    .add("page_title", StringType())

# parsing json
json_df = df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

# Функція для обробки батчів
def process_batch(batch_df, batch_id):
    if not batch_df.isEmpty():
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
    .outputMode("update") \
    .option("checkpointLocation", "/opt/app/cassandra_writer_checkpoint") \
    .trigger(processingTime="10 seconds") \
    .start()

query.awaitTermination()
import time
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, collect_list, expr, lit

def wait_until_next_hour():
    now = datetime.utcnow()
    next_hour = (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
    sleep_seconds = (next_hour - now).total_seconds()
    print(f"Sleeping {sleep_seconds:.0f} seconds until {next_hour} UTC")
    time.sleep(sleep_seconds)

def check_and_run_reports(spark):
    keyspace = "wiki_keyspace"
    last_hour_start = (datetime.utcnow().replace(minute=0, second=0, microsecond=0) - timedelta(hours=2))
    last_hour_end = last_hour_start + timedelta(hours=1)
    
    df = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table="bot_creation_stats", keyspace=keyspace).load()

    exists = df.filter((col("time_start") == last_hour_start) & (col("time_end") == last_hour_end)) \
               .limit(1).count() > 0

    if exists:
        print(f"Data for {last_hour_start} to {last_hour_end} already exists, skipping computation.")
        return False 
    else:
        print(f"Data for {last_hour_start} to {last_hour_end} does not exist, computing reports...")
        compute_and_write_reports(spark)
        return True

def compute_and_write_reports(spark):
    keyspace = "wiki_keyspace"
    source_table = "wiki_row"
    now = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
    end_time = now - timedelta(hours=1)
    start_time = end_time - timedelta(hours=6)
    
    raw_df = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table=source_table, keyspace=keyspace) \
        .load() \
        .filter((col("event_hour") >= start_time) & (col("event_hour") < end_time))
    

    # request 1
    domain_hourly_stats = raw_df.groupBy("event_hour", "domain") \
        .agg(count("*").alias("page_count")) \
        .withColumnRenamed("event_hour", "hour_bucket")
    
    domain_hourly_stats.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="domain_hourly_stats", keyspace=keyspace) \
        .mode("append") \
        .save()
    

    # request 2
    bot_stats = raw_df.filter(col("is_bot") == True) \
        .groupBy("domain") \
        .agg(count("*").alias("created_by_bots")) \
        .withColumn("time_start", lit(start_time)) \
        .withColumn("time_end", lit(end_time))
    
    bot_stats.select("time_start", "time_end", "domain", "created_by_bots").write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="bot_creation_stats", keyspace=keyspace) \
        .mode("append") \
        .save()
    
    
    # request 3
    top_users = raw_df.groupBy("user_id", "user_name") \
        .agg(
            count("*").alias("page_count"),
            collect_list("title").alias("page_titles")
        ) \
        .withColumn("time_start", lit(start_time)) \
        .withColumn("time_end", lit(end_time)) \
        .orderBy(col("page_count").desc()) \
        .limit(20)
    
    top_users.select("time_start", "time_end", "user_id", "user_name", "page_count", "page_titles").write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="top_users", keyspace=keyspace) \
        .mode("append") \
        .save()
    
    print(f"[{datetime.utcnow().isoformat()}] Reports written to Cassandra for interval {start_time} to {end_time}.")


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("CategoryAReportsLoop") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    check_and_run_reports(spark)
    
    while True:
        wait_until_next_hour()
        try:
            compute_and_write_reports(spark)
        except Exception as e:
            print(f"ERROR during processing: {e}")
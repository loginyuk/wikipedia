#!/bin/bash

echo "run containers"
docker-compose up -d

echo "waiting for services to start"
sleep 30

echo "Running Spark streaming job (filtering)..."
docker run -d --name spark-submit \
  --network hw12-network \
  -v "$(pwd)/app:/opt/app" \
  bitnami/spark:3 \
  spark-submit \
  --master spark://hw12-spark-1:7077 \
  --deploy-mode client \
  --driver-memory 2g \
  --executor-memory 2g \
  --total-executor-cores 2 \
  --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0" \
  --conf spark.sql.streaming.checkpointLocation=/opt/app/stream_filter_checkpoint \
  --conf spark.default.parallelism=2 \
  --conf spark.streaming.backpressure.enabled=true \
  --conf spark.streaming.kafka.consumer.cache.enabled=false \
  --conf spark.jars.ivy=/opt/app \
  /opt/app/input_output.py

echo "waiting 15 seconds before starting the second job..."
sleep 15


echo "Running second Spark streaming job (writing to Cassandra)..."
docker run -d --name spark-submit-2 \
  --network hw12-network \
  -v "$(pwd)/app:/opt/app" \
  bitnami/spark:3 \
  spark-submit \
  --master spark://hw12-spark-1:7077 \
  --deploy-mode client \
  --driver-memory 2g \
  --executor-memory 2g \
  --total-executor-cores 2 \
  --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,com.datastax.spark:spark-cassandra-connector_2.12:3.0.0" \
  --conf spark.sql.streaming.checkpointLocation=/opt/app/cassandra_writer_checkpoint \
  --conf spark.default.parallelism=2 \
  --conf spark.streaming.backpressure.enabled=true \
  --conf spark.streaming.kafka.consumer.cache.enabled=false \
  --conf spark.cassandra.connection.host=cassandra-node \
  --conf spark.cassandra.connection.keep_alive_ms=60000 \
  --conf spark.cassandra.output.consistency.level=LOCAL_ONE \
  --conf spark.jars.ivy=/opt/app \
  /opt/app/processed_cassandra.py

echo "all services started"


# input topic
# docker run -it --rm --name kafka-consumer-input-test --network hw12-network -e KAFKA_CFG_ZOOKEEPER_CONNECT=zookeeper-server:2181 bitnami/kafka:latest kafka-console-consumer.sh --bootstrap-server kafka-server:9092 --topic input

# processed topic
# docker run -it --rm --name kafka-consumer-processed-test --network hw12-network -e KAFKA_CFG_ZOOKEEPER_CONNECT=zookeeper-server:2181 bitnami/kafka:latest kafka-console-consumer.sh --bootstrap-server kafka-server:9092 --topic processed

# cassandra cqlsh
# docker run -it --rm --name cassandra-cqlsh --network hw12-network cassandra cqlsh cassandra-node


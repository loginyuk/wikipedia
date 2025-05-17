# to run this script with logic reading from Kafka the  and writing to Cassandra
docker run -d --name spark-row-submit \
  --network wiki-network \
  -v "$(pwd)/app:/opt/app" \
  bitnami/spark:3 \
  spark-submit \
  --master spark://wiki-spark-1:7077 \
  --deploy-mode client \
  --driver-memory 2g \
  --executor-memory 2g \
  --total-executor-cores 2 \
  --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,com.datastax.spark:spark-cassandra-connector_2.12:3.2.0" \
  --conf spark.sql.streaming.checkpointLocation=/opt/app/cassandra_writer_checkpoint \
  --conf spark.sql.streaming.checkpointLocation=/opt/app/stream_filter_checkpoint \
  --conf spark.default.parallelism=2 \
  --conf spark.streaming.backpressure.enabled=true \
  --conf spark.streaming.kafka.consumer.cache.enabled=false \
  --conf spark.cassandra.connection.host=cassandra-node \
  --conf spark.cassandra.connection.keepAliveMS=60000 \
  --conf spark.cassandra.output.consistency.level=LOCAL_ONE \
  --conf spark.jars.ivy=/opt/app \
  --conf spark.sql.adaptive.enabled=false \
  /opt/app/row_to_cassandra.py



# to run this script with logic processing the data and writing to Cassandra
docker run --rm --name spark-precompute-submit \
  --network wiki-network \
  -v "$(pwd)/app:/opt/app" \
  bitnami/spark:3 \
  spark-submit \
  --master spark://wiki-spark-1:7077 \
  --deploy-mode client \
  --driver-memory 2g \
  --executor-memory 2g \
  --total-executor-cores 2 \
  --packages com.datastax.spark:spark-cassandra-connector_2.12:3.3.0 \
  --conf spark.cassandra.connection.host=cassandra-node \
  --conf spark.cassandra.output.consistency.level=LOCAL_ONE \
  /opt/app/precompute_category_a.py

  



# input topic
# docker run -it --rm --name kafka-consumer-input-test --network wiki-network -e KAFKA_CFG_ZOOKEEPER_CONNECT=zookeeper-server:2181 bitnami/kafka:latest kafka-console-consumer.sh --bootstrap-server kafka-server:9092 --topic wiki-topic

# delete topic
# docker run -it --rm --name kafka-admin --network wiki-network \
#   bitnami/kafka:latest \
#   kafka-topics.sh \
#     --bootstrap-server kafka-server:9092 \
#     --delete \
#     --topic wiki-topic

# create topic
# docker run -it --rm --network wiki-network \
#   bitnami/kafka:latest \
#   kafka-topics.sh \
#     --bootstrap-server kafka-server:9092 \
#     --create \
#     --topic wiki-topic \
#     --partitions 1 \
#     --replication-factor 1


# cassandra cqlsh
# docker run -it --rm --name cassandra-cqlsh --network wiki-network cassandra cqlsh cassandra-node


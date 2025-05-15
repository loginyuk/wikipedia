import json
import requests
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient
import time
import logging
import backoff

# configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('wikipedia-producer')

bootstrap_servers = "kafka-server:9092"
topic_name = "input"
wiki_url = "https://stream.wikimedia.org/v2/stream/page-create"

# wait for Kafka topic to be created
def wait_for_kafka_topic():
    while True:
        try:
            admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
            topics = admin.list_topics()
            if topic_name in topics:
                logger.info(f"Topic '{topic_name}' is ready")
                return True
            else:
                logger.info(f"Waiting for topic '{topic_name}' to be created...")
        except Exception as e:
            logger.warning(f"Kafka not ready: {e}")
        time.sleep(5)

# Exponential backoff for handling connection issues
@backoff.on_exception(backoff.expo, 
                      (requests.exceptions.RequestException, 
                       requests.exceptions.ChunkedEncodingError),
                      max_tries=10,
                      jitter=backoff.full_jitter)
def stream_wikimedia_events(producer):
    logger.info("Starting stream connection to Wikimedia...")
    
    session = requests.Session()
    session.keep_alive = False  # Disable keep-alive to avoid stale connections
    
    with session.get(wiki_url, stream=True, timeout=60) as response:
        response.raise_for_status()  # Ensure we got a successful response
        
        for line in response.iter_lines():
            if line:
                decoded = line.decode('utf-8')
                if decoded.startswith("data: "):
                    try:
                        event = json.loads(decoded[6:])
                        producer.send(topic_name, event)
                        logger.info(f"Sent event to Kafka: {event['meta']['id']}")
                    except json.JSONDecodeError as e:
                        logger.error(f"Invalid JSON: {e}")
                    except KeyError as e:
                        logger.warning(f"Missing key in event: {e}")
                    except Exception as e:
                        logger.error(f"Error processing line: {e}")

if __name__ == "__main__":
    wait_for_kafka_topic()
    
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        retries=5,  # Retry sending messages if fails
        acks='all'  # Wait for all replicas to acknowledge
    )
    
    while True:
        try:
            stream_wikimedia_events(producer)
        except Exception as e:
            logger.error(f"Stream error: {e}")
            logger.info("Reconnecting in 5 seconds...")
            time.sleep(5)
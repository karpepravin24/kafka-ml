import csv
import time
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
import json

# Kafka configuration
BROKER = 'localhost:9092'
TOPIC = 'csv-topic'

# Create Kafka topic if it doesn't exist
admin_client = AdminClient({'bootstrap.servers': BROKER})

# Check if topic exists, create if not
topics = admin_client.list_topics(timeout=10).topics
if TOPIC not in topics:
    new_topic = NewTopic(TOPIC, num_partitions=3, replication_factor=1)
    admin_client.create_topics([new_topic])
    print(f"Topic '{TOPIC}' created")
else:
    print(f"Topic '{TOPIC}' already exists")

# Producer configuration
producer_conf = {'bootstrap.servers': BROKER}
producer = Producer(producer_conf)

def delivery_report(err, msg):
    """ Callback for message delivery report """
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# Path to CSV file
csv_file_path = 'twitter_sentiment.csv'

# Read CSV file and stream data every 10 seconds
with open(csv_file_path, encoding='utf-8', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    
    for index,row in enumerate(reader):
        if index >= 20:
            break
        # Convert row to string to send to Kafka
        # Extract the tweet text from the row
        tweet_text = row.get('tweet', '')
        producer.produce(TOPIC, key=str(index), value=tweet_text, callback=delivery_report)
        producer.flush()
        print(f"Sent: {tweet_text}")
        time.sleep(10)  # Wait for 10 seconds before sending the next row

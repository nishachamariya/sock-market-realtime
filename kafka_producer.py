import csv
from confluent_kafka import Producer
import json

# Kafka producer configuration
conf = {
    'bootstrap.servers': 'pkc-41p56.asia-south1.gcp.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': 'OUN3EQ4BRPF6S574',
    'sasl.password': 'Ud/1p33P2QbkdqINKbmAYTfn/3bi1X3yZ6OBBqbzqLkyboVH06BNEq4fLILkkilO'
}

# Create a Kafka producer instance
producer = Producer(conf)

# Define the topic to produce to
topic = 'reliance_data_2'

# Path to your CSV file
csv_file_path = 'RELIANCE_NS.csv'

# Define a value serializer function
def value_serializer(data):
    return json.dumps(data).encode('utf-8')

# Produce messages from the CSV file
with open(csv_file_path, newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        # Convert the row to a dictionary
        message = {key: value for key, value in row.items()}

        # Serialize the message using value_serializer
        serialized_message = value_serializer(message)

        # Produce the message to Kafka
        producer.produce(topic, value=serialized_message)
        print(f"Produced message: {serialized_message}")

# Flush the producer to ensure all messages are delivered
producer.flush()

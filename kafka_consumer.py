
from confluent_kafka import Consumer, KafkaError
import boto3
import json

# Kafka consumer configuration
consumer_conf = {
    'bootstrap.servers': 'pkc-41p56.asia-south1.gcp.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': 'xxxx',
    'sasl.password': 'xxxx',
    'group.id': 'your-consumer-group',
    'auto.offset.reset': 'latest'  # Set to 'earliest' to consume from the beginning
}

# Create a Kafka consumer instance
consumer = Consumer(consumer_conf)

# Subscribe to the topic
topic = 'reliance_data_2'
consumer.subscribe([topic])




# Create an S3 client
s3 = boto3.client('s3', aws_access_key_id='xxx',
                  aws_secret_access_key='xxx',
                  region_name='ap-south-1')

# S3 bucket
s3_bucket = 'stock-market-nisha'

# Define a value deserializer function
def value_deserializer(data):
    return json.loads(data.decode('utf-8'))

# Initialize row count
row_count = 1

# Consume messages from Kafka
while True:
    msg = consumer.poll(1.0)  # Adjust the timeout as needed

    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            # End of partition event, not an error
            continue
        else:
            print(f"Consumer error: {msg.error()}")
            break

    # Decode and deserialize the message value
    message = value_deserializer(msg.value())
    print(f"Consumed message: {message}")

    # Generate the object key with count
    object_key = f"reliance_data_{row_count}.json"

    # Upload the message to S3
    s3.put_object(Body=json.dumps(message), Bucket=s3_bucket, Key=object_key)
    print(f"Message uploaded to S3 with object key: {object_key}")

    # Increment the row count
    row_count += 1

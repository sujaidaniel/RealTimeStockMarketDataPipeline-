from kafka import KafkaConsumer
from time import sleep
from json import loads, dumps
from s3fs import S3FileSystem

# Initialize a Kafka consumer for the topic 'demo_test' with the specified server and deserialization settings
consumer = KafkaConsumer(
    'demo_test',
    bootstrap_servers=[':9092'],  # Replace with your server's IP
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)

# Set up an S3 file system instance
s3 = S3FileSystem()

# Consume messages from the Kafka topic and save them to S3 as JSON files
for count, message in enumerate(consumer):
    s3_file_path = f"s3://kafka-stock-market-tutorial-youtube-darshil/stock_market_{count}.json"
    with s3.open(s3_file_path, 'w') as file:
        json.dump(message.value, file)

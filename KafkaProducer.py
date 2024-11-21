pip install kafka-python

import pandas as pd
from kafka import KafkaProducer
from time import sleep
from json import dumps

# Set up a Kafka producer with specified server settings and data serialization
producer = KafkaProducer(
    bootstrap_servers=[':9092'],  # Update this with your server's IP
    value_serializer=lambda x: dumps(x).encode('utf-8')
)

# Send a sample message to the Kafka topic 'demo_test'
producer.send('demo_test', value={'sample_key': 'sample_value'})

# Load the dataset from a CSV file
df = pd.read_csv("data/indexProcessed.csv")
df.head()  # Display the first few rows of the dataframe

# Continuously produce and send random rows from the dataset as messages to Kafka
while True:
    random_row = df.sample(1).to_dict(orient="records")[0]  # Select a random row and convert it to a dictionary
    producer.send('demo_test', value=random_row)  # Send the random row to the 'demo_test' topic
    sleep(1)  # Pause for 1 second before sending the next message

# Ensure all messages are sent
producer.flush()

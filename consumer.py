from confluent_kafka import Consumer, KafkaException, KafkaError
import os
import pandas as pd
import json

def load_env_variables():
    """Loads environment variables from a .env file"""
    if os.path.exists(".env"):
        with open(".env") as f:
            for line in f:
                key, value = line.strip().split("=")
                os.environ[key] = value

load_env_variables()
username = os.environ.get("KAFKA_USERNAME")
password = os.environ.get("PASSWORD")
location = os.environ.get("LOCATION")

# Configuration settings
conf = {
    'bootstrap.servers': location, 
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': username,
    'sasl.password': password,
    'group.id': 'my-group',
    'auto.offset.reset': 'earliest'
}

# Create Consumer instance
consumer = Consumer(conf)

# Subscribe to topic
consumer.subscribe(['COOPData'])

# Stores the consumed messages
messages = []

# Poll for messages
try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print('End of partition reached {0}/{1}'.format(msg.topic(), msg.partition()))
            elif msg.error():
                raise KafkaException(msg.error())
        else:
            key = msg.key().decode('utf-8') if msg.key() else None
            value = msg.value().decode('utf-8')
            print('Received message: {0}'.format(msg.value().decode('utf-8')))
            messages.append({'key': key, 'value': value})
finally:
    # Close down consumer to commit final offsets.
    consumer.close()


    # Extract keys and values into separate lists
    keys = [message['key'] for message in messages]
    values = [json.loads(message['value']) for message in messages] 

    # Normalize dictionary values to a DataFrame
    values_df = pd.json_normalize(values)

    # Combine keys with the normalized values DataFrame
    result_df = pd.DataFrame({'key': keys}).join(values_df)

    # Save DataFrame to CSV
    result_df.to_csv('transformed.csv', index=False)

from confluent_kafka import Producer
import socket
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
    'sasl.password': password ,
    'client.id': socket.gethostname()
}


# Create Producer instance
producer = Producer(conf)

# Delivery callback function
def delivery_callback(err, msg):
    if err:
        print('Message failed delivery: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


# Define CSV file path and topic 
csv_file_path = "data.csv"
df = pd.read_csv(csv_file_path)
topic = "COOPData"

# Convert "yes" and "no" to 1s and 0s
df = df.replace({'Yes': 1, 'No': 0})

# Create the columns city and state by spliting Location(DMA)
df[['city', 'state']] = df['Location(DMA)'].str.split(',', expand=True)

# Drop the old location column as well as a ExposureID_2 a duplicate column
df = df.drop(columns=['Location(DMA)','ExposureID_2'])

for index, row in df.iterrows():
    key = row['SessionID']
    value = row.drop('SessionID').to_dict()  # Convert the rest of the row to a dictionary
    value_json = json.dumps(value)     # Convert the dictionary to a JSON string
    producer.produce(topic, key=key, value=value_json, callback=delivery_callback)


# Wait for any outstanding messages to be delivered and delivery reports to be received
producer.flush()

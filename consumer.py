from confluent_kafka import Consumer, KafkaException, KafkaError
import os

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
            print('Received message: {0}'.format(msg.value().decode('utf-8')))
finally:
    # Close down consumer to commit final offsets.
    consumer.close()

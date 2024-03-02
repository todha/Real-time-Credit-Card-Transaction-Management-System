import csv
import time
import random
import json
from confluent_kafka import Producer

kafka_conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'test3-topic'
}

# Create a Kafka producer
producer = Producer(kafka_conf)

# Function to send data to Kafka topic
def send_to_kafka(producer, key, data):
    topic_name = 'test3-topic'

    # Convert key to bytes
    key_bytes = str(key).encode('utf-8')

    # Convert dict to JSON string
    json_data = json.dumps(data)

    # Encode JSON string to bytes
    value = json_data.encode('utf-8')

    producer.produce(topic_name, key=key_bytes, value=value)
    producer.flush()

# CSV file path
csv_file_path = "D:\hoc\HK4_1\XLPTDLTT\data\credit_card_transactions-ibm_v2.csv"

# Offset file path
offset_file_path = "offset.txt"

# Read the offset and key from the file
try:
    with open(offset_file_path, 'r') as offset_file:
        offset = int(offset_file.read().strip())
except FileNotFoundError:
    offset = 0

key_counter = offset + 1  # Start key numbering from offset + 1

# Read CSV file and send data to Kafka
with open(csv_file_path, 'r') as file:
    reader = csv.DictReader(file)
    for i, row in enumerate(reader):
        if i < offset:
            continue  # Skip rows already processed

         # Convert row to JSON
        json_data = dict(row)

        # Add a key column to the JSON data
        json_data['key'] = key_counter

       # Send data to Kafka topic
        send_to_kafka(producer, key=key_counter, data=json_data)

        # Update the offset and key in the file
        with open(offset_file_path, 'w') as offset_file:
            offset_file.write(str(i + 1))

        key_counter += 1  # Increment key counter

        # Sleep for a random time between 1s and 3s
        sleep_time = random.uniform(1, 3)
        time.sleep(sleep_time)

# Close the Kafka producer
producer.close()

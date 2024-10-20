from confluent_kafka import Consumer, KafkaError

# Configure Kafka Consumer
conf = {
    'bootstrap.servers': "localhost:9092",
    'group.id': "my_group",  # Change the group ID if needed to reset the offset
    'auto.offset.reset': 'earliest'  # Start from the beginning if no offsets are found
}

consumer = Consumer(**conf)

# Subscribe to the Kafka topic
consumer.subscribe(['test_topic'])

# Limit for "no message" polls before exiting
empty_polls = 0
max_empty_polls = 5  # Stop after 5 consecutive empty polls

# Poll for new messages
try:
    while empty_polls < max_empty_polls:
        print("Polling for messages...")
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            print("No message received in this poll.")
            empty_polls += 1  # Increment the empty poll counter
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print("End of partition reached.")
                continue
            else:
                print(f"Error: {msg.error()}")
                break

        # Successfully received a message, reset empty poll counter
        print(f"Received message: {msg.value().decode('utf-8')}")
        empty_polls = 0  # Reset counter when a message is received

except KeyboardInterrupt:
    pass
finally:
    # Close down the consumer gracefully
    print("Closing consumer...")
    consumer.close()

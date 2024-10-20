from confluent_kafka import Producer
import time

# Configure Kafka Producer
conf = {'bootstrap.servers': "localhost:9092"}
producer = Producer(**conf)

# Define the callback to handle the delivery report
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# Produce messages in a loop
try:
    message_count = 1
    while True:
        # Send message
        producer.produce('test_topic', key='key', value=f'Hello, Kafka! {message_count}', callback=delivery_report)
        # Wait for message to be sent
        producer.flush()

        print(f"Sent message {message_count}")

        # Increment message count for differentiation
        message_count += 1

        # Sleep for 1 second before sending the next message (adjust as needed)
        time.sleep(1)

except KeyboardInterrupt:
    print("Producer stopped.")
finally:
    producer.flush()  # Ensure all messages are sent before exiting

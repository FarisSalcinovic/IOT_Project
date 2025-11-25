# ingestor.py (Gateway/Ingestor - MQTT -> Kafka)
import os
import json
import time
import signal
import sys
import paho.mqtt.client as mqtt
from confluent_kafka import Producer

# --- CONFIG ---
MQTT_HOST = os.getenv("MQTT_HOST", "mosquitto")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
KAFKA_TOPIC_RAW = "raw_temperature"
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

ROOMS = ["room1", "room2", "room3", "living_room", "office"]
TOPICS = [(f"{room}/temperature", 0) for room in ROOMS]

running = True
kafka_producer = None

def graceful_exit(signum, frame):
    global running
    print("\nGraceful shutdown (signal received).")
    running = False

signal.signal(signal.SIGINT, graceful_exit)
signal.signal(signal.SIGTERM, graceful_exit)

def delivery_report(err, msg):
    """Izvještaj o isporuci za Kafka poruke."""
    if err is not None:
        print(f"Failed to deliver message: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"Connected to MQTT broker at {MQTT_HOST}:{MQTT_PORT}")
        for t, qos in TOPICS:
            client.subscribe(t, qos)
            print(f"Subscribed to: {t}")
    else:
        print(f"Failed to connect to MQTT, return code {rc}")

def on_message(client, userdata, msg):
    topic = msg.topic
    payload = msg.payload.decode('utf-8', errors='ignore')
    room = topic.split("/")[0]

    try:
        data = json.loads(payload)
        ts = data.get("timestamp")
        val = float(data.get("value", 0))

        # Dodaj room tag u payload prije slanja na Kafku
        data["room"] = room
        kafka_payload = json.dumps(data)

        # Apsolutno kritično: Pošalji na Kafka raw topic
        kafka_producer.produce(
            KAFKA_TOPIC_RAW,
            key=room.encode('utf-8'),
            value=kafka_payload.encode('utf-8'),
            callback=delivery_report
        )
        kafka_producer.poll(0) # Asinhrono osigurava slanje
        print(f"  [KAFKA PUSH] RAW data for {room} sent to {KAFKA_TOPIC_RAW}")

    except json.JSONDecodeError:
        print(f"  [ERROR] Payload from {topic} is not valid JSON.")
    except Exception as e:
        print(f"  [ERROR] An error occurred in on_message: {e}")


def main():
    global kafka_producer
    try:
        # Inicijalizacija Kafka Producenta
        kafka_producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
        print(f"Kafka Producer initialized for {KAFKA_BOOTSTRAP_SERVERS}")
    except Exception as e:
        print(f"Error initializing Kafka Producer: {e}")
        sys.exit(1)

    mqtt_client = mqtt.Client()
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message

    try:
        mqtt_client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    except Exception as e:
        print(f"Error connecting to MQTT broker: {e}")
        sys.exit(1)

    mqtt_client.loop_start()

    while running:
        time.sleep(0.5)

    print("Flushing Kafka Producer...")
    kafka_producer.flush()
    print("Disconnecting MQTT...")
    mqtt_client.loop_stop()
    mqtt_client.disconnect()
    print("Exited cleanly.")

if __name__ == "__main__":
    main()
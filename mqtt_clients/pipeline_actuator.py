# pipeline_actuator.py (P4: Kafka Clean -> MQTT Actuation)
import os
import json
import time
import signal
import sys
import threading
# FIX: Importing KafkaError for proper End of Partition (EOF) handling
from confluent_kafka import Consumer, KafkaException, KafkaError
from confluent_kafka.admin import AdminClient
import paho.mqtt.client as mqtt

# --- CONFIG ---
KAFKA_TOPIC_CLEAN = "clean_temperature"
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_GROUP_ID = "actuator-group"
MQTT_HOST = os.getenv("MQTT_HOST", "mosquitto")
MQTT_PORT = 1883
MQTT_TOPIC_ACTUATE = "actuate/room/{room_id}"

# Granice temperature za aktivaciju
TEMP_START_HEAT = 20.0
TEMP_STOP_HEAT = 25.0

running = True


def graceful_exit(signum, frame):
    global running
    print("\nGraceful shutdown (signal received).")
    running = False


signal.signal(signal.SIGINT, graceful_exit)
signal.signal(signal.SIGTERM, graceful_exit)


def wait_for_kafka(servers):
    """Attempts to connect to Kafka until it's available."""
    print(f"Waiting for Kafka at {servers}...")
    conf = {'bootstrap.servers': servers}
    timeout = 120  # 2 minute timeout
    start_time = time.time()

    while time.time() - start_time < timeout:
        try:
            admin_client = AdminClient(conf)
            admin_client.list_topics(timeout=5)
            print("Kafka is ready!")
            return
        except Exception as e:
            if "Broker: Leader not available" in str(e) or "Local: Broker transport failure" in str(
                    e) or "Name or service not known" in str(e):
                pass
            else:
                print(f"  [WAIT] Attempt failed: {e.__class__.__name__}")
            time.sleep(5)

    print(f"[FATAL] Timeout waiting for Kafka after {timeout} seconds.")
    sys.exit(1)


def on_connect(client, userdata, flags, rc):
    """Callback for establishing MQTT connection."""
    if rc == 0:
        print(f"[MQTT] Connected successfully to {MQTT_HOST}")
    else:
        print(f"[MQTT] Connection failed with code {rc}")


def simulate_actuation(room, value, mqtt_client):
    """Determines the command based on temperature and sends it via MQTT."""
    print(f"[P4 ACTUATE] Received {value}C for {room}.")

    # ACTUATION LOGIC:
    # 1. Start Heating (HEAT_PUMP_ON) if temp is below 20.0
    # 2. Stop Heating (HEAT_PUMP_OFF) if temp is above 25.0
    # 3. Inactive (IDLE) if in between
    if value < TEMP_START_HEAT:
        command = "HEAT_PUMP_ON"
    elif value > TEMP_STOP_HEAT:
        command = "HEAT_PUMP_OFF"
    else:
        command = "IDLE"

    payload = json.dumps({"command": command, "current_temp": value})
    topic = MQTT_TOPIC_ACTUATE.format(room_id=room)

    # Sending command back to MQTT broker
    # Simulating slow process (e.g., waiting for hardware response)
    print(f"[P4 ACTUATE] Simulating command process for {room} ({command}). Sleeping for 5s.")
    time.sleep(5)

    mqtt_client.publish(topic, payload)
    print(f"[P4 ACTUATE] Command sent to {topic}: {command} (Temp: {value}C)")


def main():
    try:
        # --- STEP 1: Waiting for Kafka ---
        wait_for_kafka(KAFKA_BOOTSTRAP_SERVERS)

        # STEP 2: Initialize MQTT client
        mqtt_client = mqtt.Client(client_id="actuator_pipeline")
        mqtt_client.on_connect = on_connect
        mqtt_client.connect(MQTT_HOST, MQTT_PORT, 60)
        mqtt_client.loop_start()

        # STEP 3: Initialize Kafka Consumer
        consumer_conf = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': KAFKA_GROUP_ID,
            'auto.offset.reset': 'earliest'
        }
        consumer = Consumer(consumer_conf)
        consumer.subscribe([KAFKA_TOPIC_CLEAN])
        print(f"Kafka Consumer for P4 subscribed to {KAFKA_TOPIC_CLEAN}")

    except Exception as e:
        print(f"[FATAL] Initialization error: {e}")
        # Ensure loop_stop() is called even on error
        if 'mqtt_client' in locals() and mqtt_client.is_connected():
            mqtt_client.loop_stop()
        sys.exit(1)

    while running:
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            continue

        error = msg.error()
        if error:
            # FIX: Using KafkaError to check for EOF
            if error.code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"[ERROR] Consumer error: {error}")
                continue

        try:
            payload = msg.value().decode('utf-8')
            data = json.loads(payload)

            val = data.get("value")
            room = data.get("room")

            if val is not None and room:
                val = float(val)
                print(f"[P4: ACTUATOR] Received clean {val}C for {room}. Offloading task...")
                # Offload slow actuation operations to a separate thread
                actuator_thread = threading.Thread(
                    target=simulate_actuation,
                    args=(room, val, mqtt_client)
                )
                actuator_thread.start()
            else:
                print(f"[WARN] Incomplete data: {data}")

        except json.JSONDecodeError:
            print(f"[ERROR] Could not decode JSON: {payload}")
        except Exception as e:
            print(f"[FATAL] Unhandled exception during message processing: {e}")

    # Closing connections
    consumer.close()
    mqtt_client.loop_stop()
    mqtt_client.disconnect()
    print("P4 Exited cleanly.")


if __name__ == "__main__":
    main()
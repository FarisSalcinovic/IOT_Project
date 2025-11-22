# subscriber.py
import os
import json
import time
import signal
import sys
import csv
import paho.mqtt.client as mqtt

MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
TOPICS = [
    ("room1/temperature", 0),
    ("room2/temperature", 0),
    ("room3/temperature", 0),
    ("living_room/temperature", 0),
    ("office/temperature", 0),
]

CSV_FILE = "raw_temperatures.csv"

running = True

def graceful_exit(signum, frame):
    global running
    print("\nGraceful shutdown (signal received).")
    running = False

signal.signal(signal.SIGINT, graceful_exit)
signal.signal(signal.SIGTERM, graceful_exit)

# Initialize CSV with header if not exists
if not os.path.exists(CSV_FILE):
    with open(CSV_FILE, mode='w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "room", "value"])

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"Connected to MQTT broker at {MQTT_HOST}:{MQTT_PORT}")
        for t, qos in TOPICS:
            client.subscribe(t, qos)
            print(f"Subscribed to: {t}")
    else:
        print(f"Failed to connect, return code {rc}")

def on_message(client, userdata, msg):
    payload = msg.payload.decode('utf-8', errors='ignore')
    print(f"\n[{msg.topic}] {payload}")
    try:
        data = json.loads(payload)
        ts = data.get("timestamp")
        val = data.get("value")
        room = msg.topic.split("/")[0]
        print(f"  Parsed -> timestamp: {ts}, room: {room}, value: {val}")

        # Append to CSV
        with open(CSV_FILE, mode='a', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([ts, room, val])
    except json.JSONDecodeError:
        print("  Payload is not valid JSON.")

def main():
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    try:
        client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    except Exception as e:
        print(f"Error connecting to MQTT broker at {MQTT_HOST}:{MQTT_PORT} -> {e}")
        sys.exit(1)

    client.loop_start()

    while running:
        time.sleep(0.5)

    print("Disconnecting...")
    client.loop_stop()
    client.disconnect()
    print("Exited cleanly.")

if __name__ == "__main__":
    main()

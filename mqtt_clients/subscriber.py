# subscriber.py
import os
import json
import time
import signal
import sys
import csv
import paho.mqtt.client as mqtt

# --- CONFIG: promijeni ako je potrebno ---
MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))

ROOMS = ["room1", "room2", "room3", "living_room", "office"]
TOPICS = [(f"{room}/temperature", 0) for room in ROOMS]

# Base folder za CSV fajlove u kontejneru
BASE_DATA_FOLDER = "/mqtt/data"

running = True

def graceful_exit(signum, frame):
    global running
    print("\nGraceful shutdown (signal received).")
    running = False

signal.signal(signal.SIGINT, graceful_exit)
signal.signal(signal.SIGTERM, graceful_exit)

# Kreiraj folder i fajlove za svaku sobu ako ne postoje
for room in ROOMS:
    folder = os.path.join(BASE_DATA_FOLDER, room)
    os.makedirs(folder, exist_ok=True)
    for file_name in ["raw.csv", "clean.csv"]:
        path = os.path.join(folder, file_name)
        if not os.path.exists(path):
            with open(path, "w", newline="") as f:
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

        # --- P1: RAW ---
        raw_file = os.path.join(BASE_DATA_FOLDER, room, "raw.csv")
        with open(raw_file, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([ts, room, val])

        # --- P2: CLEAN (izbaci vrijednosti 100) ---
        if val != 100:
            clean_file = os.path.join(BASE_DATA_FOLDER, room, "clean.csv")
            with open(clean_file, "a", newline="") as f:
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

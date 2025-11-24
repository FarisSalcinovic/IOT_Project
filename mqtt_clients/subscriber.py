# subscriber.py
import os
import json
import time
import signal
import sys
import csv
import statistics
import paho.mqtt.client as mqtt
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

# --- CONFIG ---
MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))

ROOMS = ["room1", "room2", "room3", "living_room", "office"]
TOPICS = [(f"{room}/temperature", 0) for room in ROOMS]

# CSV base folder
BASE_DATA_FOLDER = "/mqtt/data"

# InfluxDB config
INFLUX_URL = os.getenv("INFLUX_URL", "http://influxdb:8086")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN", "gOofN2uycwMDf8JlTjWvj70pr5pugmM7paUpTT9eY8wIVXW0OmU5_4ZN3afnuHtmOMJ2CNMswU725NXcIEMZAA==")  # tvoj token
INFLUX_ORG = os.getenv("INFLUX_ORG", "my-org")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "raw_data")

influx_client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
write_api = influx_client.write_api(write_options=SYNCHRONOUS)

running = True

def graceful_exit(signum, frame):
    global running
    print("\nGraceful shutdown (signal received).")
    running = False

signal.signal(signal.SIGINT, graceful_exit)
signal.signal(signal.SIGTERM, graceful_exit)

# Kreiraj folder i CSV fajlove za svaku sobu ako ne postoje
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
    room = msg.topic.split("/")[0]
    print(f"\n[{msg.topic}] {payload}")

    try:
        data = json.loads(payload)
        ts = data.get("timestamp")
        val = float(data.get("value", 0))
        print(f"  Parsed -> timestamp: {ts}, room: {room}, value: {val}")

        # --- P1: RAW CSV ---
        raw_file = os.path.join(BASE_DATA_FOLDER, room, "raw.csv")
        with open(raw_file, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([ts, room, val])

        # --- P1: RAW -> InfluxDB ---
        point = Point("temperature") \
            .tag("room", room) \
            .field("value", val) \
            .time(ts)
        write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)

        # --- P2: CLEAN CSV (izbaci 100) ---
        if val != 100:
            clean_file = os.path.join(BASE_DATA_FOLDER, room, "clean.csv")
            with open(clean_file, "a", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([ts, room, val])

            # --- P3: ACTUATE ---
            command = None
            if val < 20:
                command = {"status": 1}  # start
                print(f"[P3] {room}: Temperature {val}°C < 20 -> START heat pump")
            elif val > 25:
                command = {"status": 0}  # stop
                print(f"[P3] {room}: Temperature {val}°C > 25 -> STOP heat pump")

            if command:
                client.publish(f"{room}/heatpump", json.dumps(command))

            # --- P3: AGGREGATE ---
            all_clean_values = []
            for r in ROOMS:
                clean_file_r = os.path.join(BASE_DATA_FOLDER, r, "clean.csv")
                try:
                    with open(clean_file_r, newline="") as f:
                        reader = csv.DictReader(f)
                        vals = [float(row["value"]) for row in reader]
                        all_clean_values.extend(vals)
                except FileNotFoundError:
                    continue

            if all_clean_values:
                mean_temp = statistics.mean(all_clean_values)
                print(f"[P3] Aggregation: mean temperature of all rooms = {mean_temp:.2f}°C")

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

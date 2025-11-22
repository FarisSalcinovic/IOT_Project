import datetime
import time
import os
import paho.mqtt.client as mqtt
from timeseries_get import TemperatureSimulator
import json

sensor_topic_status = os.getenv("SENSOR_TOPIC_STATUS", "room1/heatpump")
sensor_topic_data = os.getenv("SENSOR_TOPIC_DATA", "room1/temperature")
mqtt_host = os.getenv("MQTT_HOST", "localhost")
heatpump_status = 0

def heatpump_status_change(c, userdata, message):
    global heatpump_status
    try:
        v = json.loads(message.payload.decode("utf-8"))
        heatpump_status = int(v["status"])
        print(f"Heatpump status updated: {heatpump_status}")
    except:
        pass

client = mqtt.Client()
client.on_message = heatpump_status_change

try:
    client.connect(mqtt_host, 1883, 60)
except Exception as e:
    print(f"Cannot connect to MQTT broker at {mqtt_host}:1883 -> {e}")
    exit(1)

client.subscribe(sensor_topic_status)
client.loop_start()

ts_gen = TemperatureSimulator(datetime.datetime.now())

while True:
    reading = ts_gen.get_temperature(datetime.datetime.now(), heatpump_status)
    to_send = json.dumps({"timestamp": reading[0].isoformat(), "value": reading[1]})
    client.publish(sensor_topic_data, to_send)
    print(f"Published: {to_send}")
    time.sleep(1)

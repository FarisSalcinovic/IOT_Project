import datetime
import time
import os
import json
import paho.mqtt.client as mqtt
from timeseries_get import TemperatureSimulator

# sobe
rooms = ["room1", "room2", "room3", "living_room", "office"]
mqtt_host = os.getenv("MQTT_HOST", "mosquitto")
heatpump_status = 0

client = mqtt.Client()
client.connect(mqtt_host, 1883, 60)
client.loop_start()

# generator temperature
ts_gen = TemperatureSimulator(datetime.datetime.now())

while True:
    for room in rooms:
        reading = ts_gen.get_temperature(datetime.datetime.now(), heatpump_status)
        payload = json.dumps({"timestamp": reading[0].isoformat(), "value": reading[1]})
        client.publish(f"{room}/temperature", payload)
    time.sleep(1)  # 1 sekunda simulira 1 minutu

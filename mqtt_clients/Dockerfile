FROM python:3.9-slim
RUN apt-get update && apt-get install -y build-essential librdkafka-dev python3-dev && rm -rf /var/lib/apt/lists/* && pip install paho-mqtt influxdb-client confluent-kafka statistics
WORKDIR /app
COPY . /app
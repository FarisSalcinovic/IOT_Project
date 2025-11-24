FROM python:3.10.0-slim
WORKDIR /mqtt_clients
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY subscriber.py subscriber.py
CMD python3 -u subscriber.py


FROM python:3.10.0-slim
WORKDIR /sensor
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY controller.py controller.py
COPY timeseries_get.py timeseries_get.py
CMD python3 -u controller.py

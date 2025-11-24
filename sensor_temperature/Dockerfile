FROM python:3.10-slim
WORKDIR /mqtt
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY controller.py controller.py
COPY timeseries_get.py timeseries_get.py
CMD ["python3", "-u", "controller.py"]

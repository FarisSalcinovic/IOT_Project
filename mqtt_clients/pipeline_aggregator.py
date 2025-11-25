# pipeline_aggregator.py (P3: Kafka Clean -> InfluxDB Clean + Mean)
import os
import json
import time
import signal
import sys
# Ažuriran uvoz: dodajemo KafkaError
from confluent_kafka import Consumer, KafkaException, KafkaError
from confluent_kafka.admin import AdminClient
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime, timezone

# --- CONFIG ---
KAFKA_TOPIC_CLEAN = "clean_temperature"
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_GROUP_ID = "aggregator-group"

# InfluxDB config
INFLUX_URL = os.getenv("INFLUX_URL", "http://influxdb:8086")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN", "new-dev-token")
INFLUX_ORG = os.getenv("INFLUX_ORG", "my-org")
INFLUX_BUCKET_CLEAN = os.getenv("INFLUX_BUCKET_CLEAN", "clean_data")
INFLUX_BUCKET_AGG = os.getenv("INFLUX_BUCKET_AGG", "clean_data")

# Agregacija - Tumbling Window State
AGG_WINDOW_SECONDS = 10
aggregation_list = []
last_aggregation_time = time.time()
aggregation_lock = False

running = True


def graceful_exit(signum, frame):
    global running
    print("\nGraceful shutdown (signal received).")
    running = False


signal.signal(signal.SIGINT, graceful_exit)
signal.signal(signal.SIGTERM, graceful_exit)


def wait_for_kafka(servers):
    """Pokušava se povezati s Kafkom dok ne bude dostupna."""
    print(f"Waiting for Kafka at {servers}...")
    conf = {'bootstrap.servers': servers}
    timeout = 120
    start_time = time.time()

    while time.time() - start_time < timeout:
        try:
            admin_client = AdminClient(conf)
            admin_client.list_topics(timeout=5)
            print("Kafka je spremna!")
            return
        except Exception as e:
            # Bolja obrada Kafka čekanja
            error_msg = str(e)
            if "Broker: Leader not available" in error_msg or "Local: Broker transport failure" in error_msg or "Name or service not known" in error_msg:
                pass  # Čekanje je i dalje potrebno
            else:
                print(f"  [WAIT] Attempt failed ({e.__class__.__name__}): {error_msg}")
            time.sleep(5)

    print(f"[FATAL] Timeout waiting for Kafka after {timeout} seconds.")
    sys.exit(1)


def ensure_bucket_exists(client, bucket_name, org_name):
    """Provjerava postojanje kante i kreira je ako ne postoji."""
    print(f"[INFLUX_INIT] Checking for InfluxDB bucket '{bucket_name}'...")
    try:
        # Koristimo health check da provjerimo da li je InfluxDB dostupan prije kreiranja kante
        health = client.health()
        if health.status != "pass":
            raise Exception(f"InfluxDB Health check failed: {health.message}")
        print("[INFLUX_INIT] InfluxDB service is healthy.")

        bucket_api = client.buckets_api()
        bucket = bucket_api.find_bucket_by_name(bucket_name)

        if bucket is None:
            org_api = client.organizations_api()

            # ISPRAVKA: find_organization_by_name uzrokuje gresku.
            # Koristimo robustnu metodu dohvaćanja svih organizacija i filtriranja.
            organizations = org_api.find_organizations()
            org = next((o for o in organizations if o.name == org_name), None)

            if org is None:
                raise Exception(f"Organization '{org_name}' not found. Check INFLUX_ORG configuration.")

            print(f"[INFLUX_INIT] Attempting to create bucket '{bucket_name}' in org '{org_name}'...")
            client.buckets_api().create_bucket(bucket_name=bucket_name, org=org)
            print(f"[INFLUX_INIT] InfluxDB bucket '{bucket_name}' created successfully.")
        else:
            print(f"[INFLUX_INIT] InfluxDB bucket '{bucket_name}' already exists.")
    except Exception as e:
        print(f"[FATAL_INFLUX] Failed to ensure bucket exists. CHECK INFLUX_URL, TOKEN, and ORG! Error: {e}")
        raise  # Ponovo bacamo iznimku da bi se zaustavila inicijalizacija u main()


def perform_aggregation(write_api):
    """
    Izračunava prosječnu temperaturu doma (svih soba) u tekućem prozoru
    i sprema rezultat kao 'home_mean_temperature'.
    """
    global aggregation_list, last_aggregation_time, aggregation_lock
    if aggregation_lock:
        return

    aggregation_lock = True

    if aggregation_list:
        try:
            mean_value = sum(aggregation_list) / len(aggregation_list)
            current_time_ns = datetime.now(timezone.utc).timestamp() * 10 ** 9

            agg_point = Point("home_mean_temperature") \
                .tag("user", "home") \
                .field("mean_temp", round(mean_value, 2)) \
                .field("count", len(aggregation_list)) \
                .time(int(current_time_ns), WritePrecision.NS)

            write_api.write(bucket=INFLUX_BUCKET_AGG, org=INFLUX_ORG, record=agg_point)

            print(
                f"[P3: AGGREGATOR] Agregacija: Prosjek doma: {round(mean_value, 2)}C ({len(aggregation_list)} točaka).")

        except Exception as e:
            print(f"[ERROR] Greška pri izračunu ili pisanju agregacije: {e}")

        aggregation_list = []
        last_aggregation_time = time.time()

    aggregation_lock = False


def main():
    global aggregation_list, last_aggregation_time
    print("[P3 START] Starting pipeline_aggregator.py...")

    try:
        wait_for_kafka(KAFKA_BOOTSTRAP_SERVERS)

        # Inicijalizacija InfluxDB klijenta
        print(f"[P3 INIT] Initializing InfluxDB client: URL={INFLUX_URL}, ORG={INFLUX_ORG}")
        influx_client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)

        # Kreiranje kante 'clean_data' za čiste i agregirane podatke
        ensure_bucket_exists(influx_client, INFLUX_BUCKET_CLEAN, INFLUX_ORG)

        write_api = influx_client.write_api(write_options=SYNCHRONOUS)
        print("[P3 INIT] InfluxDB Write API initialized.")

        # Inicijalizacija Kafka Potrošača
        consumer_conf = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': KAFKA_GROUP_ID,
            'auto.offset.reset': 'earliest'
        }
        consumer = Consumer(consumer_conf)
        consumer.subscribe([KAFKA_TOPIC_CLEAN])
        print(f"[P3 INIT] Kafka Consumer pretplaćen na {KAFKA_TOPIC_CLEAN}")

    except Exception as e:
        print(f"[FATAL] Greška pri inicijalizaciji P3. Izlazak iz procesa. Detalji: {e}")
        sys.exit(1)

    while running:
        if time.time() - last_aggregation_time >= AGG_WINDOW_SECONDS:
            perform_aggregation(write_api)

        msg = consumer.poll(timeout=1.0)

        if msg is None:
            continue

        # Poboljšana provjera greške koristeći KafkaError
        error = msg.error()
        if error:
            if error.code() == KafkaError._PARTITION_EOF:
                # Normalna situacija: nema više poruka u particiji
                continue
            else:
                # Bilo koja druga stvarna greška
                print(f"[ERROR] Greška potrošača: {error}")
                continue

        try:
            clean_payload = msg.value().decode('utf-8')
            data = json.loads(clean_payload)

            ts = data.get("timestamp")
            val = data.get("value")
            room = data.get("room")
            raw_val = data.get("raw_value")

            if ts and val is not None and room:
                point = Point("temperature_clean") \
                    .tag("room", room) \
                    .field("value", float(val)) \
                    .field("raw_value", float(raw_val)) \
                    .time(ts, WritePrecision.NS)
                write_api.write(bucket=INFLUX_BUCKET_CLEAN, org=INFLUX_ORG, record=point)
                print(f"[P3: CLEAN SAVER] Spremljena očišćena temp {val}C iz {room} u InfluxDB.")

                aggregation_list.append(float(val))

            else:
                print(f"[WARN] Nepotpuni podaci: {data}")

        except json.JSONDecodeError:
            print(f"[ERROR] Neuspješno dekodiranje JSON-a: {clean_payload}")
        except Exception as e:
            print(f"[ERROR] Greška pri pisanju u InfluxDB (P3 runtime): {e}")

    perform_aggregation(write_api)
    consumer.close()
    influx_client.close()
    print("P3 Exited cleanly.")


if __name__ == "__main__":
    main()
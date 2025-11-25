# pipeline_raw_saver.py (P1: Kafka -> InfluxDB Raw)
import os
import json
import time
import signal
import sys
from confluent_kafka import Consumer, KafkaException
from confluent_kafka.admin import AdminClient
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
# Uklonjen je uvoz za InfluxDBError da bi se izbjegao ModuleNotFoundError.
# Koristi se opći 'except Exception'.

# --- CONFIG ---
KAFKA_TOPIC_RAW = "raw_temperature"
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_GROUP_ID = "raw-saver-group"

# InfluxDB config
INFLUX_URL = os.getenv("INFLUX_URL", "http://influxdb:8086")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN", "new-dev-token")
INFLUX_ORG = os.getenv("INFLUX_ORG", "my-org")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "raw_data")

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
    timeout = 120  # 2 minute timeout
    start_time = time.time()

    while time.time() - start_time < timeout:
        try:
            # Koristimo AdminClient za robustnu provjeru konekcije
            admin_client = AdminClient(conf)
            # Pokušavamo dobiti listu brokera (ako uspije, Kafka je dostupna)
            admin_client.list_topics(timeout=5)
            print("Kafka je spremna!")
            return
        except Exception as e:
            # Ispis greške samo ako nije TimeoutError, da ne spamamo log
            if "Broker: Leader not available" in str(e) or "Local: Broker transport failure" in str(
                    e) or "Name or service not known" in str(e):
                pass
            else:
                print(f"  [WAIT] Attempt failed: {e.__class__.__name__}")
            time.sleep(5)

    print(f"[FATAL] Timeout waiting for Kafka after {timeout} seconds.")
    sys.exit(1)


def ensure_bucket_exists(client, bucket_name, org_name):
    """Provjerava postojanje kante i kreira je ako ne postoji."""
    print(f"Checking for InfluxDB bucket '{bucket_name}'...")
    try:
        bucket_api = client.buckets_api()
        # Traženje kante po imenu
        bucket = bucket_api.find_bucket_by_name(bucket_name)

        if bucket is None:
            # Ako kanta ne postoji, kreiraj je
            org_api = client.organizations_api()
            org = org_api.find_organization_by_name(org_name)

            if org is None:
                raise Exception(f"Organization '{org_name}' not found.")

            # Kreiranje kante
            client.buckets_api().create_bucket(bucket_name=bucket_name, org=org)
            print(f"InfluxDB bucket '{bucket_name}' created successfully.")
        else:
            print(f"InfluxDB bucket '{bucket_name}' already exists.")
    except Exception as e:
        print(f"[FATAL] Failed to ensure bucket exists: {e}")
        sys.exit(1)


def main():
    try:
        # --- NOVI KORAK: Čekanje na Kafku ---
        wait_for_kafka(KAFKA_BOOTSTRAP_SERVERS)

        # Inicijalizacija InfluxDB klijenta
        influx_client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)

        # --- KOREKCIJA: Osiguraj da kanta postoji prije pisanja ---
        ensure_bucket_exists(influx_client, INFLUX_BUCKET, INFLUX_ORG)
        # ---------------------------------------------------------

        write_api = influx_client.write_api(write_options=SYNCHRONOUS)

        # Inicijalizacija Kafka Potrošača
        consumer_conf = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': KAFKA_GROUP_ID,
            'auto.offset.reset': 'earliest'
        }
        consumer = Consumer(consumer_conf)
        consumer.subscribe([KAFKA_TOPIC_RAW])
        print(f"Kafka Consumer za P1 je pretplaćen na {KAFKA_TOPIC_RAW}")

    except Exception as e:
        print(f"Greška pri inicijalizaciji: {e}")
        sys.exit(1)

    while running:
        # Pollaj poruke sa timeutom od 1 sekunde
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaException._PARTITION_EOF:
                # Kraj particije - normalno za batch obradu
                continue
            else:
                print(f"[ERROR] Greška potrošača: {msg.error()}")
                continue

        # Procesuiranje poruke
        try:
            raw_payload = msg.value().decode('utf-8')
            data = json.loads(raw_payload)

            ts = data.get("timestamp")
            val = data.get("value")
            room = data.get("room")

            if ts and val is not None and room:
                # P1: Snimanje RAW podataka u InfluxDB
                point = Point("temperature_raw") \
                    .tag("room", room) \
                    .field("value", float(val)) \
                    .time(ts, WritePrecision.NS)
                write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
                print(f"[P1: RAW SAVER] Spremljena sirova temperatura {val}C iz {room} u InfluxDB.")
            else:
                print(f"[WARN] Nepotpuni podaci: {data}")

        except json.JSONDecodeError:
            print(f"[ERROR] Neuspješno dekodiranje JSON-a: {raw_payload}")
        except Exception as e:
            # Opća iznimka za InfluxDB greške pisanja
            print(f"[ERROR] Greška pri pisanju u bazu podataka (ili neobrađena greška poruke): {e}")

    # Zatvaranje konekcija
    consumer.close()
    influx_client.close()
    print("P1 Izašao čisto.")


if __name__ == "__main__":
    main()
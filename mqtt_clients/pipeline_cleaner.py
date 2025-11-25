# pipeline_cleaner.py (P2: Kafka Raw -> Kafka Clean)
import os
import json
import time
import signal
import sys
from confluent_kafka import Consumer, Producer, KafkaException
from confluent_kafka.admin import AdminClient
# Uklonjen je uvoz InfluxDBError i InfluxDBClient jer P2 ne komunicira sa InfluxDB.

# --- CONFIG ---
KAFKA_TOPIC_RAW = "raw_temperature"
KAFKA_TOPIC_CLEAN = "clean_temperature"
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_GROUP_ID = "cleaner-group"

# State Management za Moving Average (po sobi)
ROOM_DATA = {}
WINDOW_SIZE = 5  # Moving average na zadnjih 5 mjerenja

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
            if "Broker: Leader not available" in str(e) or "Local: Broker transport failure" in str(e) or "Name or service not known" in str(e):
                pass
            else:
                print(f"  [WAIT] Attempt failed: {e.__class__.__name__}")
            time.sleep(5)

    print(f"[FATAL] Timeout waiting for Kafka after {timeout} seconds.")
    sys.exit(1)


def process_temperature(room, value):
    """
    Primjenjuje validaciju i čišćenje podataka:
    1. Gruba provjera outliera (0°C do 40°C).
    2. Eksplicitno uklanjanje vrijednosti od 100°C (projektni zahtjev).
    3. Primjena pokretnog prosjeka (Moving Average).
    """
    global ROOM_DATA

    # 1. Projektni zahtjev: Uklanjanje tačno 100 stupnjeva
    if value == 100.0:
        print(f"[CLEANER] REJECTED: Temperatura {value}C ({room}) je tačno 100.0. Uklonjena prema zahtjevu projekta.")
        return None

    # 2. Opšta Validacija (realističan opseg)
    if not (0.0 <= value <= 40.0):
        print(f"[CLEANER] WARN: Temperatura {value}C za {room} je izvan normalnog opsega (0-40C). Odbacivanje.")
        return None

    # 3. Moving Average za izglađivanje šuma
    if room not in ROOM_DATA:
        ROOM_DATA[room] = []

    ROOM_DATA[room].append(value)

    if len(ROOM_DATA[room]) > WINDOW_SIZE:
        ROOM_DATA[room].pop(0)

    avg_value = sum(ROOM_DATA[room]) / len(ROOM_DATA[room])

    return round(avg_value, 2)


def main():
    try:
        wait_for_kafka(KAFKA_BOOTSTRAP_SERVERS)

        consumer_conf = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': KAFKA_GROUP_ID,
            'auto.offset.reset': 'earliest'
        }
        consumer = Consumer(consumer_conf)
        consumer.subscribe([KAFKA_TOPIC_RAW])
        print(f"Kafka Consumer za P2 (Cleaner) pretplaćen na {KAFKA_TOPIC_RAW}")

        producer_conf = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS}
        producer = Producer(producer_conf)
        print(f"Kafka Producer za P2 (Cleaner) će slati na {KAFKA_TOPIC_CLEAN}")

    except Exception as e:
        print(f"Greška pri inicijalizaciji P2: {e}")
        sys.exit(1)


    while running:
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaException._PARTITION_EOF:
                continue
            else:
                print(f"[ERROR] Greška potrošača: {msg.error()}")
                continue

        try:
            raw_payload = msg.value().decode('utf-8')
            data = json.loads(raw_payload)

            ts = data.get("timestamp")
            raw_val = data.get("value")
            room = data.get("room")

            if ts and raw_val is not None and room:
                clean_val = process_temperature(room, float(raw_val))

                if clean_val is not None:
                    # Kreiranje CLEAN poruke
                    clean_data = {
                        "timestamp": ts,
                        "value": clean_val,
                        "room": room,
                        "raw_value": float(raw_val)
                    }
                    clean_payload = json.dumps(clean_data)

                    # Slanje CLEAN poruke
                    producer.produce(KAFKA_TOPIC_CLEAN, key=room, value=clean_payload)
                    producer.poll(0)

                    print(f"[P2: CLEANER] Očišćeno: {float(raw_val)}C -> {clean_val}C ({room}). Poslano u '{KAFKA_TOPIC_CLEAN}'.")
            else:
                print(f"[WARN] Nepotpuni podaci: {data}")

        except json.JSONDecodeError:
            print(f"[ERROR] Neuspješno dekodiranje JSON-a: {raw_payload}")
        except Exception as e:
            print(f"[ERROR] Neočekivana greška tokom obrade poruke u P2: {e}")

    producer.flush()
    consumer.close()
    print("P2 Exited cleanly.")


if __name__ == "__main__":
    main()
import logging
import os
import signal
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from confluent_kafka import Producer

from app.generator import SongPlayEventGenerator

logging.basicConfig(level=logging.INFO, format="[%(threadName)s] %(message)s")

shutdown_event = threading.Event()


def handle_shutdown(signum, frame):
    logging.info(f"Received signal {signum}. Shutting down...")
    shutdown_event.set()


def produce_events():
    kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    kafka_topic = os.getenv("KAFKA_TOPIC")

    if kafka_bootstrap_servers is None:
        raise ValueError("Kafka bootstrap servers parameter is required")
    if kafka_topic is None:
        raise ValueError("Kafka topic parameter is required")

    kafka_conf = {
        "bootstrap.servers": kafka_bootstrap_servers,
        "client.id": socket.gethostname(),
    }

    producer = Producer(kafka_conf)

    batch_size = 10_000
    sleep_ms = 50

    try:
        event_generator = SongPlayEventGenerator(100_000)

        while not shutdown_event.is_set():
            for _ in range(batch_size):
                try:
                    event = event_generator.generate().model_dump_json()
                    producer.produce(kafka_topic, value=event)
                except BufferError as e:
                    logging.error(f"Buffer full: {e}")
                    producer.poll(1)
            
            logging.info(f"Produced {batch_size} events")
            time.sleep(sleep_ms / 1000)
    finally:
        producer.flush()


def main():
    signal.signal(signal.SIGINT, handle_shutdown)  # Ctrl+C
    signal.signal(signal.SIGTERM, handle_shutdown)  # kill <pid>

    producer_threads = int(os.getenv("PRODUCER_THREADS", "4"))

    with ThreadPoolExecutor(max_workers=producer_threads) as pool:
        futures = [
            pool.submit(produce_events)
            for _ in range(producer_threads)
        ]

        for future in futures:
            try:
                future.result()
            except Exception as e:
                logging.exception(f"Worker thread failed: {e}")

if __name__ == "__main__":
    main()

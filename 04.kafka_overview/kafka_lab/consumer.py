#!/usr/bin/env python3
"""Consume Kafka messages, clean the text, and write results to a file."""

# Student: Lisa EL GABTENI

from __future__ import annotations

import argparse
import re
from pathlib import Path

from confluent_kafka import Consumer

DEFAULT_TOPIC = "book_lines"
DEFAULT_OUTPUT_PATH = "cleaned_output.txt"


def clean_message(raw_message: str) -> str:
    text = raw_message.lower().replace("’", "'")
    text = re.sub(r"[^a-z0-9\s']", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def consume_messages(topic_name: str, output_path: str) -> None:
    conf = {
        "bootstrap.servers": "localhost:9092",
        "group.id": "book_cleaner_group",
        "auto.offset.reset": "earliest",
    }

    consumer = Consumer(conf)
    consumer.subscribe([topic_name])

    output_file = Path(output_path)
    written_lines = 0

    try:
        with output_file.open("w", encoding="utf-8") as destination:
            while True:
                msg = consumer.poll(1.0)

                if msg is None:
                    continue

                if msg.error():
                    print(f"Consumer error: {msg.error()}")
                    continue

                raw_value = msg.value().decode("utf-8", errors="replace")
                cleaned_value = clean_message(raw_value)

                if cleaned_value:
                    destination.write(cleaned_value + "\n")
                    written_lines += 1
                    print(cleaned_value)
    except KeyboardInterrupt:
        print(f"\nStopped consumption. Wrote {written_lines} cleaned lines to '{output_file}'.")
    finally:
        consumer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Consume Kafka messages and persist cleaned text.")
    parser.add_argument("--topic", default=DEFAULT_TOPIC, help="Kafka topic to consume from.")
    parser.add_argument("--output", default=DEFAULT_OUTPUT_PATH, help="Destination text file.")
    args = parser.parse_args()

    consume_messages(args.topic, args.output)

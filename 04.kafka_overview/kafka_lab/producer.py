#!/usr/bin/env python3
"""Publish lines from a Gutenberg book into a Kafka topic."""

# Student: Lisa EL GABTENI

from __future__ import annotations

import argparse
import socket
import time
from pathlib import Path
from urllib.request import urlopen

from confluent_kafka import Producer

DEFAULT_TOPIC = "book_lines"
DEFAULT_BOOK_PATH = "book.txt"
DEFAULT_BOOK_URL = "https://www.gutenberg.org/cache/epub/1342/pg1342.txt"


def download_book(url: str, output_path: str) -> str:
    path = Path(output_path)
    if path.exists() and path.stat().st_size > 0:
        print(f"Book already available at '{path}'.")
        return str(path)

    print(f"Downloading book from '{url}' to '{path}'...")
    with urlopen(url, timeout=30) as response:
        content = response.read()

    path.write_bytes(content)
    print(f"Saved {len(content)} bytes to '{path}'.")
    return str(path)


def iter_book_lines(book_path: str):
    with open(book_path, "r", encoding="utf-8", errors="replace") as book_file:
        for raw_line in book_file:
            cleaned_line = raw_line.strip()
            if cleaned_line:
                yield cleaned_line


def publish_book(topic_name: str, book_path: str, delay_seconds: float = 0.0) -> None:
    conf = {
        "bootstrap.servers": "localhost:9092",
        "client.id": socket.gethostname(),
    }
    producer = Producer(conf)

    published = 0
    try:
        for line in iter_book_lines(book_path):
            producer.produce(topic=topic_name, value=line)
            published += 1
            if delay_seconds > 0:
                time.sleep(delay_seconds)

        producer.flush()
        print(f"Published {published} lines to topic '{topic_name}'.")
    finally:
        producer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Send a book's lines to a Kafka topic.")
    parser.add_argument("--topic", default=DEFAULT_TOPIC, help="Kafka topic to write to.")
    parser.add_argument("--book", default=DEFAULT_BOOK_PATH, help="Path to the book text file.")
    parser.add_argument(
        "--url",
        default=DEFAULT_BOOK_URL,
        help="Project Gutenberg URL used when the book file is not present.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.0,
        help="Optional sleep time in seconds between messages.",
    )
    args = parser.parse_args()

    if not Path(args.book).exists():
        args.book = download_book(args.url, args.book)

    publish_book(args.topic, args.book, delay_seconds=args.delay)

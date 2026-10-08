#!/usr/bin/env python3
"""Kafka admin helpers for the Kafka lab."""

# Student: Lisa EL GABTENI

from __future__ import annotations

import argparse

from confluent_kafka.admin import AdminClient, NewTopic


DEFAULT_TOPIC = "book_lines"
DEFAULT_PARTITIONS = 1
DEFAULT_REPLICATION_FACTOR = 1


def get_admin_client() -> AdminClient:
    config = {"bootstrap.servers": "localhost:9092"}
    return AdminClient(config)


def create_topic(
    topic_name: str = DEFAULT_TOPIC,
    num_partitions: int = DEFAULT_PARTITIONS,
    replication_factor: int = DEFAULT_REPLICATION_FACTOR,
) -> None:
    admin_client = get_admin_client()
    future_map = admin_client.create_topics(
        [
            NewTopic(
                topic_name,
                num_partitions=num_partitions,
                replication_factor=replication_factor,
            )
        ]
    )

    for topic, future in future_map.items():
        try:
            future.result()
            print(f"Topic '{topic}' created successfully.")
        except Exception as exc:  # pragma: no cover - depends on Kafka runtime
            print(f"Failed to create topic '{topic}': {exc}")


def list_topics() -> None:
    admin_client = get_admin_client()
    metadata = admin_client.list_topics(timeout=5)
    topics = sorted(metadata.topics)

    print("Available topics:")
    if not topics:
        print("- none")
    else:
        for topic in topics:
            print(f"- {topic}")


def delete_topic(topic_name: str) -> None:
    admin_client = get_admin_client()
    future_map = admin_client.delete_topics([topic_name])

    for topic, future in future_map.items():
        try:
            future.result()
            print(f"Topic '{topic}' deleted successfully.")
        except Exception as exc:  # pragma: no cover - depends on Kafka runtime
            print(f"Failed to delete topic '{topic}': {exc}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create, list, or delete Kafka topics.")
    parser.add_argument("--create", action="store_true", help="Create the default topic.")
    parser.add_argument("--delete", action="store_true", help="Delete the default topic.")
    parser.add_argument("--list", action="store_true", help="List all topics.")
    parser.add_argument("--topic", default=DEFAULT_TOPIC, help="Topic name to manage.")
    args = parser.parse_args()

    if args.create:
        create_topic(topic_name=args.topic)
    if args.list:
        list_topics()
    if args.delete:
        delete_topic(args.topic)

    if not any([args.create, args.list, args.delete]):
        print("No action selected. Use --create, --list, or --delete.")

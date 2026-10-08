# Kafka Demo and lab

This lab is a practical introduction to Kafka using Python and Docker. The goal is to build a minimal producer/topic/consumer workflow and to understand how messages flow through a distributed messaging system.

## Objectives

By the end of this lab, you should be able to:

- start a Kafka broker locally with Docker;
- create a Kafka topic;
- produce records to a topic;
- consume records from a topic;
- clean and store incoming text data for further processing.

## Prerequisites

- Docker installed and running;
- Python 3.9 or later;
- a virtual environment for the project;
- the `confluent-kafka` Python package.

## Environment setup

1. Create and activate a new Python environment.
2. Install the dependency:

```bash
python -m pip install confluent-kafka
```

3. Copy the files from the Kafka lab folder into your working directory.

## Start Kafka with Docker

Pull and run the official Kafka image:

```bash
docker pull apache/kafka-native:4.1.1
docker run -p 9092:9092 apache/kafka-native:4.1.1
```

This starts a local Kafka broker listening on port 9092.

> Important: each new terminal must activate the Python environment before running the scripts.

## Step 1: create a topic

Use the admin script to create the topic:

```bash
python admin.py --create --topic book_lines
```

You can check the topics with:

```bash
python admin.py --list
```

## Step 2: send book lines to Kafka

The producer script downloads a public-domain book file if it is missing, then reads it line by line and produces one message per line.

```bash
python producer.py --topic book_lines --book book.txt
```

Optional delay between messages:

```bash
python producer.py --topic book_lines --book book.txt --delay 0.1
```

## Step 3: consume messages

Open a second terminal and launch the consumer:

```bash
python consumer.py --topic book_lines --output cleaned_output.txt
```

The consumer reads each Kafka message, cleans the text, and writes the result to a file.

## Step 4: text cleaning example

The cleaning process includes:

- lowercase conversion;
- punctuation removal;
- whitespace normalization;
- discarding empty lines.

Example:

```text
Original: "Hello, world! This is a Kafka message."
Cleaned: "hello world this is a kafka message"
```

## Evaluation criteria

A strong solution should show:

- correct Kafka topic creation;
- functioning producer and consumer scripts;
- clean and readable Python code;
- text processing logic with a practical output file;
- good understanding of Kafka concepts such as topics, partitions, producers, consumers, and offsets.

## Deliverable

The final result should include:

- a working Kafka producer;
- a working Kafka consumer;
- a clean text output file;
- a short explanation of how the architecture works.

This lab is intentionally simple, but it reflects the real pattern used in production stream-processing systems: data is generated, sent to Kafka, consumed, transformed, and stored.

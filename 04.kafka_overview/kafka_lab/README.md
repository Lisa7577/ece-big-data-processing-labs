# Kafka lab solution

This folder contains a working Kafka example built around the same ideas as the course lab:

- create a topic,
- publish book lines to Kafka,
- consume the messages,
- clean the text,
- write the cleaned content to a file.

## Prerequisites

- Docker
- Python 3.9+
- A Python environment with the `confluent-kafka` package installed

Install the dependency:

```bash
python -m pip install -r requirements.txt
```

## Start Kafka

```bash
docker run -p 9092:9092 apache/kafka-native:4.1.1
```

## 1) Create the topic

```bash
python admin.py --create --topic book_lines
```

You can also list topics:

```bash
python admin.py --list
```

## 2) Publish the book to Kafka

If the book file does not exist, the script downloads a public-domain text automatically from Project Gutenberg.

```bash
python producer.py --topic book_lines --book book.txt
```

Optional delay between messages:

```bash
python producer.py --topic book_lines --book book.txt --delay 0.1
```

## 3) Consume and clean messages

```bash
python consumer.py --topic book_lines --output cleaned_output.txt
```

The consumer writes cleaned lines to the output file. It lowercases the text, removes punctuation, collapses repeated spaces, and saves each message on a separate line.

## 4) Stop the consumer

Use Ctrl+C to stop the consumer loop.

## Notes

- The default topic is `book_lines`.
- The book is downloaded from Project Gutenberg when missing.
- The code is intentionally simple and easy to understand for a lab submission.

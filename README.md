# stream-mx-inspector

## Project Overview

This project, `stream-mx-inspector`, is designed to process and route streaming data from Kafka. It consists of two main packages: `raw_message_processor` and `parsed_message_writer`. The system is built for asynchronous operations using `aiokafka` and `uvloop`, with structured logging provided by `loguru`.

## Installation

Install dependencies using `uv`:
```bash
uv sync
```
If you don't have `uv` installed, follow the [uv documentation](https://github.com/astral-sh/uv) for installation instructions.

## Packages

### 1. `raw_message_processor`

This package is responsible for consuming raw messages from an initial Kafka topic, parsing them based on their content, and producing them to different topics for further processing.

#### Workflow

1.  **Consume Messages**: An `AIOKafkaConsumer` subscribes to a specified source topic to fetch raw messages in batches.
2.  **Select Parser**: For each message, it inspects the content to find a `test_code`. This code is used to dynamically select the appropriate parser for the message format.
3.  **Parse Data**: The selected parser transforms the raw message (e.g., a string of key-value pairs) into a structured Python dictionary.
4.  **Serialize to Avro**: The parsed dictionary is serialized into two different Avro schemas:
    *   **Partial Schema**: Contains a subset of the most critical data (`master_topic`).
    *   **Full Schema**: Contains the complete, detailed information (`detail_topic`).
5.  **Produce to Topics**: An `AIOKafkaProducer` sends the two serialized Avro messages to their respective Kafka topics. This routing allows downstream consumers to access either summarized or complete data as needed.
6.  **Error Handling**: If a message cannot be parsed, the error is logged along with the raw message content, and the process continues without interruption.

#### Running the Processor

Run the service from the project root directory:
```bash
python -m raw_message_processor
```
Configuration is managed via files in the `config/loader` directory.

### 2. `parsed_message_writer`

(Details for this package can be added here once implemented.)

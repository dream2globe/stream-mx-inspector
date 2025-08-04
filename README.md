# stream-mx-inspector

## Project Overview

This project, `stream-mx-inspector`, is designed to process and route streaming data from Kafka. It consists of two main packages: `raw_message_processor` and `parsed_message_writer`. The system is built for asynchronous operations using `aiokafka` and `uvloop`, with structured logging provided by `loguru`.

## Installation

Install dependencies using `uv`:
```bash
uv sync
```
If you don't have `uv` installed, follow the [uv documentation](https://github.com/astral-sh/uv) for installation instructions.



For local testing, you can use `docker-compose` to set up a complete Kafka environment. The configuration is defined in the `docker-compose.yml` file and includes:

- **fast-data-dev**: An all-in-one Kafka environment from Landoop, providing a Broker, Zookeeper, Schema Registry, and REST Proxy.
- **AKHQ**: A web-based GUI for managing and monitoring your Kafka cluster. AKHQ's configuration is located in the `config/akhq/application.yml` file.

### How to Use

1.  **Start the Services:**
    Run the following command from the project root directory to start all services in detached mode:
    ```bash
    docker-compose up -d
    ```

2.  **Service Endpoints:**
    Once the containers are running, you can access the services at these endpoints:
    *   **Kafka Broker**: `127.0.0.1:9092`
    *   **Schema Registry**: `http://127.0.0.1:8081`
    *   **AKHQ Web UI**: `http://127.0.0.1:8080`
    *   **Landoop UI (fast-data-dev)**: `http://127.0.0.1:3030`

3.  **Stopping the Environment:**
    To stop and remove the containers, use:
    ```bash
    docker-compose down
    ```

### Registering Avro Schemas with the Local Schema Registry

The `curl` command for registering schemas can sometimes fail. A more reliable method is to use the AKHQ web interface.

1.  **Access AKHQ**: Open your web browser and navigate to the AKHQ UI at `http://localhost:8080`.
2.  **Navigate to Schemas**: In the AKHQ sidebar, go to the "Schemas" section.
3.  **Register `full_message.json`**:
    *   Click on "New Schema".
    *   Set the **Subject** to `mx-inspector-log-full`.
    *   Copy the entire content of the `schema/full_message.json` file and paste it into the "Schema" text area.
    *   Click "Save". The schema will be registered with ID `10`.
4.  **Register `master_message.json`**:
    *   Click on "New Schema" again.
    *   Set the **Subject** to `mx-inspector-log-master`.
    *   Copy the entire content of the `schema/master_message.json` file and paste it into the "Schema" text area.
    *   Click "Save". The schema will be registered with ID `11`.

Once the environment is running and schemas are registered with IDs `10` and `11`, you can start the project's services to test the full data pipeline.

## Packages

### 1. `raw_message_processor`

This package is responsible for consuming raw messages from an initial Kafka topic, parsing them based on their content, and producing them to different topics for further processing.

#### Workflow

1.  **Consume Messages**: An `AIOKafkaConsumer` subscribes to a specified source topic to fetch raw messages in batches.
2.  **Select Parser**: For each message, it inspects the content to find a `test_code`. This code is used to dynamically select the appropriate parser for the message format.
3.  **Parse Data**: The selected parser transforms the raw message (e.g., a string of key-value pairs) into a structured Python dictionary.
4.  **Serialize to Avro**: The parsed dictionary is serialized into two different Avro schemas:
    *   **Master Schema**: Contains a subset of the most critical data.
    *   **Full Schema**: Contains the complete, detailed information.
5.  **Produce to Topics**: An `AIOKafkaProducer` sends the two serialized Avro messages to their respective Kafka topics. This routing allows downstream consumers to access either summarized or complete data as needed.
6.  **Error Handling**: If a message cannot be parsed, the error is logged along with the raw message content, and the process continues without interruption.

#### Running the Processor

Run the service from the project root directory:
```bash
python -m raw_message_processor
```
Configuration is managed via files in the `config/processor` directory.

### 2. `parsed_message_writer`

(Details for this package can be added here once implemented.)

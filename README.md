# stream-mx-inspector

## Project Overview

This project, `stream-mx-inspector`, is designed to process streaming data from Kafka. It utilizes `aiokafka` for asynchronous Kafka operations and `loguru` for logging. The core functionality resides in the `raw_message_processor` module, which includes parsers (like `inspector_log_parser`) to handle different message types and a main application for consuming, processing, and producing messages to different Kafka topics. The project is built with Python 3.12 or higher and manages dependencies using `uv`.

## Installation

Install dependencies using `uv`: 
```bash
uv sync
```
If you don't have `uv` installed, follow the [uv documentation](https://github.com/astral-sh/uv) for installation instructions.

## Running the Project

Run the project using the Python module command from the root directory:
```bash
python -m raw_message_processor
```
Ensure Kafka is running and configured (`raw_message_processor/configs/config.toml`).

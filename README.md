# stream-mx-inspector

## Project Overview

This project, `stream-mx-inspector`, is designed to process and route streaming data from Kafka. It consists of two main packages: `raw_message_processor` and `parsed-message-connector`. The system is built for asynchronous operations using `aiokafka` and `uvloop`, with structured logging provided by `loguru`.

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

### 2. `parsed-message-connector`

This package provides the configuration and scripts to deploy a Kafka Connect cluster on Minikube. It is designed to sink data from Kafka topics to external systems, such as S3, using the S3 Sink Connector.

---

## Installation

Install dependencies using `uv`:
```bash
uv sync
```
If you don't have `uv` installed, follow the [uv documentation](https://github.com/astral-sh/uv) for installation instructions.

---

## Local Development Environment (Docker Compose)

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

A reliable method is to use the AKHQ web interface.

1.  **Access AKHQ**: Open your web browser and navigate to the AKHQ UI at `http://localhost:8080`.
2.  **Navigate to Schemas**: In the AKHQ sidebar, go to the "Schemas" section.
3.  **Register `full_message.json`**:
    *   Click on "New Schema".
    *   Set the **Subject** to `mx-inspector-log-full`.
    *   Copy the entire content of the `schema/full_message.json` file and paste it into the "Schema" text area.
    *   Click "Save".
4.  **Register `master_message.json`**:
    *   Click on "New Schema" again.
    *   Set the **Subject** to `mx-inspector-log-master`.
    *   Copy the entire content of the `schema/master_message.json` file and paste it into the "Schema" text area.
    *   Click "Save".

---

## Kubernetes Deployment Guide (Minikube & Kafka Connect)

This guide walks through setting up Kafka, Schema Registry, Kafka Connect, and AKHQ in a Minikube environment.

### Step 1: Prepare Development Environment (Minikube & Docker)

**1. Start Minikube Cluster**
```bash
minikube start --cpus=8 --memory=16g
```
Check status:
```bash
minikube status
```

**2. Prepare Project Directory and Clean Up Data**
Navigate to the project directory and clean up previous Docker containers and volumes.
```bash
cd /path/to/your/project/stream-mx-inspector
docker-compose down
sudo rm -rf /path/to/your/data/minio # Adjust the path as needed
mkdir -p /path/to/your/data/minio # Adjust the path as needed
```

**3. Run Kafka Broker and Minio**
```bash
docker-compose up -d
```
Check that containers are up:
```bash
docker-compose ps
```

### Step 2: Deploy Kafka Cluster Resources

**1. Create Kafka Namespace**
```bash
kubectl create namespace kafka
```
Check status:
```bash
kubectl get namespace kafka
```

**2. Deploy Schema Registry**
Deploy the schema registry within the `kafka` namespace.
```bash
kubectl apply -f parsed-message-connector/schema-registry.yaml -n kafka
```
Check pod status until it's `Running`:
```bash
kubectl get pods -n kafka -w
```

### Step 3: Deploy Kafka Connect Cluster (Strimzi)

**1. Add and Install Strimzi Helm Repository**
```bash
helm install strimzi-operator oci://quay.io/strimzi-helm/strimzi-kafka-operator \
  --version 0.47.0 \
  -n kafka
```

**2. Apply Strimzi CRDs (Custom Resource Definitions)**
```bash
kubectl apply -f https://github.com/strimzi/strimzi-kafka-operator/releases/download/0.47.0/strimzi-crds-0.47.0.yaml -n kafka
```
Check that the `strimzi-cluster-operator` pod is `Running`:
```bash
kubectl get pods -n kafka
```

### Step 4: Deploy Kafka Connect with S3 Sink Connector

**1. Build Custom Docker Image**
Build a custom Kafka Connect image that includes the S3 sink connector plugin.
```bash
docker build -t custom-connect-s3:latest -f parsed-message-connector/Dockerfile .
```
Verify the image was created:
```bash
docker images | grep custom-connect-s3
```

**2. Load Image into Minikube**
Make the local Docker image available to the Minikube cluster.
```bash
minikube image load custom-connect-s3:latest
```

**3. Deploy Kafka Connect Cluster**
```bash
kubectl apply -f parsed-message-connector/kafka-connect.yaml -n kafka
```
Wait for all three `my-connect-cluster-connect` pods to be `Running`:
```bash
kubectl get pods -n kafka -w
```

### Step 5: Install and Access AKHQ Management Tool

**1. Add and Install AKHQ Helm Chart**
```bash
helm repo add akhq https://akhq.io/
helm install akhq akhq/akhq --namespace kafka -f parsed-message-connector/akhq-values.yaml
```
Wait for the `akhq` pod to be `Running`:
```bash
kubectl get pods -n kafka -w
```

**2. Access AKHQ Dashboard via Port Forwarding**
```bash
export POD_NAME=$(kubectl get pods --namespace kafka -l "app.kubernetes.io/name=akhq,app.kubernetes.io/instance=akhq" -o jsonpath="{.items[0].metadata.name}")
kubectl port-forward --namespace kafka $POD_NAME 8080:8080
```
Access the AKHQ dashboard in your browser at `http://127.0.0.1:8080`.

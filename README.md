# Kafka Sidecar Microservice

This project provides a sidecar microservice that attaches to applications, handling all interactions with Kafka and the Avro schema registry. The sidecar is designed to simplify the integration of Kafka with your applications by offloading Kafka communication and schema management to a dedicated service.

## Features

- **Kafka Integration**: Listens to Kafka topics, triggers HTTP routes, and sends messages back to Kafka.
- **Avro Schema Support**: Handles Avro schema registry interactions.
- **Error Handling**: Configurable error handling via environment variables:
  - Option to terminate the service on errors.
  - Option to log errors and continue processing.
- **Commit on Success**: Commits Kafka offsets only if the HTTP route responds with a `200` status and the message is successfully sent to the topic.

## Getting Started

### Prerequisites

- **Kafka Cluster**: A running Kafka cluster with necessary topics.
- **Schema Registry**: Avro schema registry for schema management.

### Configuration

The service can be configured via environment variables:

- `KAFKA_BROKERS`: Comma-separated list of Kafka broker addresses. (default: `localhost:9092`) 
- `DEBUG`: Set to `true` to enable debug logging, or `false` to show only errors.(default: `false`) 
- `KAFKA_CONSUMER_GROUP_ID`: Kafka consumer group ID. (required)
- `KAFKA_TOPICS`: Comma-separated list of Kafka topics to listen to. (required)
- `SCHEMA_REGISTRY_URL`: URL of the Avro schema registry. (default: `http://localhost:8081`) 
- `HTTP_ROUTE`: The HTTP route that will handle the POST request. (required)
- `TERMINATE_ON_ERROR`:  Set to `true` to stop the service on errors, or `false` to log errors and continue. (default: `true`)
- `COMMIT_ON_SUCCESS`: Set to `true` to commit Kafka offsets only on successful processing. (default: `true`)
- `STARTUP_DELAY`: Delay in seconds before starting the service. (default: `0`)

Example:

```bash
export DEBUG=true
export KAFKA_CONSUMER_GROUP_ID="my-consumer-group"
export KAFKA_BROKERS="broker1:9092,broker2:9092"
export KAFKA_TOPICS="topic1,topic2"
export SCHEMA_REGISTRY_URL="http://schema-registry:8081"
export HTTP_ROUTE="http://localhost:8080/process"
export TERMINATE_ON_ERROR="true"
export STARTUP_DELAY="2"
export COMMIT_ON_SUCCESS="true"
# Kafka-ML Integration

## Objective

This project demonstrates how to establish a Kafka stream that feeds real-time data into a machine learning model for immediate processing and prediction. The system includes a Kafka producer that streams data and a Kafka consumer that processes the data using an ML model.

## Table of Contents

- [Objective](#objective)
- [Pre-requisites](#pre-requisites)
- [1. Setting Up Kafka](#1-setting-up-kafka)
  - [Docker Compose File](#docker-compose-file)
  - [Running Kafka and Zookeeper](#running-kafka-and-zookeeper)
  - [Installing Confluent Kafka](#installing-confluent-kafka)
- [2. Preparing the ML Model](#2-preparing-the-ml-model)
- [3. Implementing the Kafka Producer](#3-implementing-the-kafka-producer)
- [4. Implementing the Kafka Consumer and ML Model Integration](#4-implementing-the-kafka-consumer-and-ml-model-integration)
- [5. Testing the System](#5-testing-the-system)
- [6. Monitoring and Error Handling](#6-monitoring-and-error-handling)
- [7. Closing All Services and Connections](#7-closing-all-services-and-connections)
- [Figures](#figures)
- [License](#license)

## Pre-requisites

- Docker installed
- Python 3.x installed
- PostgreSQL installed and running
- Pre-trained Scikit-learn ML model
- Kafka and Zookeeper Docker images

## 1. Setting Up Kafka

To set up Kafka and Zookeeper using Docker, follow these steps:

### Docker Compose File

Create a `docker-compose.yml` file in the root of your project directory and paste the following code:

```yaml
version: '2'

services:
  zookeeper:
    image: wurstmeister/zookeeper:latest
    ports:
      - "2181:2181"

  kafka:
    image: wurstmeister/kafka:latest
    ports:
      - "9092:9092"
    expose:
      - "9093"
    environment:
      KAFKA_ADVERTISED_LISTENERS: INSIDE://kafka:9093,OUTSIDE://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: INSIDE:PLAINTEXT,OUTSIDE:PLAINTEXT
      KAFKA_LISTENERS: INSIDE://0.0.0.0:9093,OUTSIDE://0.0.0.0:9092
      KAFKA_INTER_BROKER_LISTENER_NAME: INSIDE
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_CREATE_TOPICS: "my-topic:1:1"
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock

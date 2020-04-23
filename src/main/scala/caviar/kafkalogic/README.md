## Install Kafka

* Download the kafka .tgz (Scala 2.12  - kafka_2.12-2.4.0.tgz (asc, sha512)) from: https://kafka.apache.org/downloads

* Extract to a desired location

## Run the Kafka Server

* cd to: kafka_2.12-2.4.0
* open a terminal and run: bin/zookeeper-server-start.sh config/zookeeper.properties
* in another terminal run: bin/kafka-server-start.sh config/server.properties

## Create a 3 partition topic

A three partition topic is needed as for now the LocalCoordinator creates 3 Learners. Each learner gets assigned to one partition and starts consuming Examples. 

From within the kafka folder (kafka_2.12-2.4.0) run:

* bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3 --topic ExamplesTopic

## Run the Runner main


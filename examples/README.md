# Examples to run doge-datagen on k8s

Below examples assumes that you have minikube cluster running with helmfile applied from 
https://gitlab.com/getindata/streaming-labs/k8s-workloads
and all services are exposed via port forwarding.

```shell
kubectl port-forward -n confluent service/zookeeper 2181 &
kubectl port-forward -n confluent service/kafka-bootstrap-np 9092 &
kubectl port-forward -n confluent service/kafka-0-np 31001:9092 &
kubectl port-forward -n confluent service/schemaregistry 8081 &
kubectl port-forward -n vvp service/vvp-ververica-platform 8080:80 &
kubectl port-forward -n postgresql service/postgresql 5432 &
export PGPASSWORD=$(kubectl get secret postgresql --namespace postgresql -o jsonpath="{.data.postgresql-password}" | base64 --decode)
```

## Kafka example
1. Run Kafka instance

2. Create test topic

```shell
kafka-topics.sh --bootstrap-server localhost:9092 --create --topic test_topic --partitions 1 --replication-factor 1
```

3. Run doge_kafka_example.py


## Kafka Avro example
1. Run Kafka and Schema registry instance

2. Create test topic

```shell
kafka-topics.sh --bootstrap-server localhost:9092 --create --topic test_avro_topic --partitions 1 --replication-factor 1
```

3. Run console consumer

```shell
$ cat ~/.confluent/local.conf 
# Kafka
bootstrap.servers=localhost:9092

# Confluent Schema Registry
schema.registry.url=http://localhost:8081

$ kafka-avro-console-consumer --topic test_avro_topic --consumer.config ~/.confluent/local.conf --bootstrap-server localhost:9092
```

4. Run doge_kafka_avro_example.py

## DB Example
1. Create events table

`docker run -it --rm --network host -e PGPASSWORD=$PGPASSWORD postgres psql -h localhost -U postgres -c "create table events (user_id int, balance int, loan_balance int, event varchar(50), timestamp int)"`

2. Run doge_db_example.py

## Demo example
1. Create tables and topics
 
```shell
docker run -it --rm --network host -e PGPASSWORD=$PGPASSWORD postgres psql -h localhost -U postgres -c "create table loan (timestamp int, user_id int, loan int)"
docker run -it --rm --network host -e PGPASSWORD=$PGPASSWORD postgres psql -h localhost -U postgres -c "create table balance (timestamp int, user_id int, balance int)"

kafka-topics.sh --bootstrap-server localhost:9092 --create --topic trx --partitions 1 --replication-factor 1
kafka-topics.sh --bootstrap-server localhost:9092 --create --topic clickstream --partitions 1 --replication-factor 1
```

2. Run doge_demo.py


# Examples to run doge-datagen on docker

Requirements
* docker installed 
* docker compose installed

In terminal cd to examples/docker

Start technology stack

```shell
make start_techstack
```

Update ports in example files accordingly:
* kafka 29092

Create topic:
* from terminal

```shell
make create_topic topicname=test_topic
```

* from akhq UI. In web browser open http://localhost:28080 and create and investigate topics

To view database tables Dbeaver (https://dbeaver.io/) can be used






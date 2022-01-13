# Examples

## Kafka example
1. Run Kafka instance

2. Create test topic

`kafka-topics.sh --bootstrap-server localhost:9092 --create --topic test_topic --partitions 2 --replication-factor 1`

3. Run doge_kafka_example.py

## DB Example
1. Run PostgreSQL instance

`docker run --name postgres -e POSTGRES_PASSWORD=postgres -d -p 5432:5432 postgres`

2. Create events table

`docker run -it --rm --network host -e PGPASSWORD=postgres postgres psql -h localhost -U postgres -c "create table events (user_id int, balance int, loan_balance int, event varchar(50), timestamp int)"`

3. Run doge_db_example.py
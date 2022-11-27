start_kafka:
	docker-compose -f docker-compose-kafka.yml up -d

stop_kafka:
	docker-compose -f docker-compose-kafka.yml down

create_topic:
	docker-compose -f docker-compose-kafka.yml exec kafka kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic $(topicname)



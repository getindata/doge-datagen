start_techstack:
	docker-compose -f docker-compose-techstack.yml up -d

stop_techstack:
	docker-compose -f docker-compose-techstack.yml down

create_topic:
	docker-compose -f docker-compose-techstack.yml exec kafka kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic $(topicname)



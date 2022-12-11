from typing import Dict, Any

from doge_datagen import Transition, KafkaAvroSinkFactory
from examples.doge_example_common import create_example_data_online_generator, User


def key_function(subject: User, transition: Transition) -> Dict[str, Any]:
    return {'key': str(subject.user_id)}


def value_function(timestamp: int, subject: User, transition: Transition) -> Dict[str, Any]:
    value = {
        'timestamp': timestamp,
        'user': {
            'userId': str(subject.user_id),
            'balance': str(subject.balance),
            'loanBalance': str(subject.loan_balance)
        },
        'event': transition.trigger
    }
    return value


def get_schema(schema_path):
    with open(schema_path) as f:
        return f.read()


if __name__ == '__main__':
    key_schema = get_schema('./avro/Key.avsc')
    event_schema = get_schema('./avro/Event.avsc')

    bootstrup_servers = ['localhost:9092']
    client_id = 'doge-kafka-example'

    # Kafka Configuration - for OCI Streaming
    kafka_conf = {
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': 'username',
        'sasl.password': 'pswd'
    }

    schema_registry_url = 'http://localhost:8081'

    topic_name = 'test_avro_topic'

    factory = KafkaAvroSinkFactory(bootstrup_servers, schema_registry_url, client_id, kafka_conf)
    sink = factory.create(topic_name, key_function, key_schema, value_function, event_schema)

    datagen = create_example_data_online_generator(sink)

    datagen.start()

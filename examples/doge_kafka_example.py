import json

from doge_datagen import Transition, KafkaSinkFactory
from examples.doge_example_common import create_example_data_online_generator, User


def key_function(subject: User, transition: Transition) -> str:
    return str(subject.user_id)


def value_function(timestamp: int, subject: User, transition: Transition) -> str:
    value = {
        'timestamp': timestamp,
        'user': {
            'user_id': subject.user_id,
            'balance': subject.balance,
            'loan_balance': subject.loan_balance
        },
        'event': transition.trigger
    }
    return json.dumps(value)


if __name__ == '__main__':

    bootstrap_servers = ['localhost:9092']
    client_id = 'doge-kafka-example'

    # Kafka Additional Configuration - optional
    kafka_conf = {
        'bootstrap.servers': ','.join(bootstrap_servers),
        'client.id': client_id,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': 'username',
        'sasl.password': 'pswd'
    }

    topic_name = 'test_topic'
    
    factory = KafkaSinkFactory(conf=kafka_conf)
    sink = factory.create(topic_name, key_function, value_function)

    datagen = create_example_data_online_generator(sink)

    datagen.start()

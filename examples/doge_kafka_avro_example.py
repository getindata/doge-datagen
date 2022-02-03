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

    factory = KafkaAvroSinkFactory(['localhost:9092'], 'http://localhost:8081', 'doge-kafka-example')
    sink = factory.create('test_avro_topic', key_function, key_schema, value_function, event_schema)

    datagen = create_example_data_online_generator(sink)

    datagen.start()

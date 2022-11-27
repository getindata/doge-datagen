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

    # Kafka Configuration - for OCI Streaming
    kafkaConf = {
        'bootstrap.servers': 'cell-1.streaming.<region>.oci.oraclecloud.com:9092', #usually of the form cell-1.streaming.<region>.oci.oraclecloud.com:9092  
        'client.id': 'doge-kafka-example',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': 'tenancyid/userid/streampoolid',
        'sasl.password': 'pswd'
    }

    # Kafka Configuration - basic setup
    kafkaConf = {
        'bootstrap.servers': 'localhost:9092',  
        'client.id': 'doge-kafka-example'
    }
    
    factory = KafkaSinkFactory(kafkaConf)
    sink = factory.create('test_topic', key_function, value_function)

    datagen = create_example_data_online_generator(sink)

    datagen.start()

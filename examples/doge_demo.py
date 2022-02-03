import os
from typing import Dict, Any

from doge_datagen import DataOnlineGenerator, KafkaAvroSinkFactory, DbSinkFactory, Subject, Transition

from examples.doge_example_common import income_callback, spending_callback, take_loan_callback, UserFactory, User
from examples.doge_kafka_avro_example import key_function, get_schema

# Common
kafka_avro_factory = KafkaAvroSinkFactory(['localhost:9092'], 'http://localhost:8081', 'doge-demo')
db_pass = os.getenv('PGPASSWORD', 'postgres')
db_factory = DbSinkFactory('postgresql://postgres:{}@localhost:5432/postgres'.format(db_pass))
key_schema = get_schema('./avro/Key.avsc')

# TRX definition
trx_event_schema = get_schema('./avro/Trx.avsc')


def trx_value_function(timestamp: int, subject: User, transition: Transition) -> Dict[str, Any]:
    value = {
        'timestamp': timestamp,
        'user_id': str(subject.user_id),
        'amount': subject.last_payment
    }
    return value


trx_sink = kafka_avro_factory.create("trx", key_function, key_schema, trx_value_function, trx_event_schema)


# Clickstream definition
clickstream_event_schema = get_schema('./avro/Clickstream.avsc')


def clickstream_value_function(timestamp: int, subject: User, transition: Transition) -> Dict[str, Any]:
    value = {
        'timestamp': timestamp,
        'user_id': str(subject.user_id),
        'event': transition.trigger
    }
    return value


clickstream_sink = kafka_avro_factory.create("clickstream", key_function, key_schema,
                                             clickstream_value_function, clickstream_event_schema)


# Balance definition
def balance_mapper_function(timestamp: int, subject: User, transition: Transition) -> Dict[str, Any]:
    row = {
        'timestamp': timestamp,
        'user_id': str(subject.user_id),
        'balance': subject.balance,
    }
    return row


balance_sink = db_factory.create("balance", balance_mapper_function)


# Loan definition
def loan_mapper_function(timestamp: int, subject: User, transition: Transition) -> Dict[str, Any]:
    row = {
        'timestamp': timestamp,
        'user_id': str(subject.user_id),
        'loan': subject.loan_balance,
    }
    return row


loan_sink = db_factory.create("loan", loan_mapper_function)


# Doge configuration
def create_example_data_online_generator():
    datagen = DataOnlineGenerator(['offline', 'online', 'loan_screen'], 'offline', UserFactory(), 10, 60000, 1000)
    datagen.add_transition('income', 'offline', 'offline', 0.01,
                           action_callback=income_callback, event_sinks=[balance_sink])
    datagen.add_transition('spending', 'offline', 'offline', 0.1,
                           action_callback=spending_callback, event_sinks=[trx_sink, balance_sink])
    datagen.add_transition('login', 'offline', 'online', 0.1, event_sinks=[clickstream_sink])
    datagen.add_transition('logout', 'online', 'offline', 70, event_sinks=[])
    datagen.add_transition('open_loan_screen ', 'online', 'loan_screen', 30, event_sinks=[clickstream_sink])
    datagen.add_transition('close_loan_screen', 'loan_screen', 'online', 40, event_sinks=[clickstream_sink])
    datagen.add_transition('take_loan', 'loan_screen', 'online', 10,
                           action_callback=take_loan_callback, event_sinks=[clickstream_sink, loan_sink, balance_sink])
    return datagen


if __name__ == '__main__':
    datagen = create_example_data_online_generator()
    datagen.start()

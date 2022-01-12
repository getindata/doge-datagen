from random import randrange
from doge import *
from kafka_callback_factory import KafkaCallbackFactory
import json


def income_callback(subject: Subject, transition: Transition):
    subject.balance += randrange(500,2000)


def spending_callback(subject: Subject, transition: Transition):
    subject.balance -= randrange(10, 100)


def wire_out_callback(subject: Subject, transition: Transition):
    subject.balance -= randrange(10, 100)


def take_loan_callback(subject: Subject, transition: Transition):
    subject.loan_balance += 10000
    subject.balance += 10000


class UserFactory(SubjectFactory):
    def __init__(self):
        super().__init__()
        self.current_id = 0

    def create(self):
        user = SubjectFactory.create(self)
        user.user_id = self.current_id
        user.balance = randrange(0, 1000)
        user.loan_balance = 0
        self.current_id += 1
        return user


def key_function(subject: Subject, transition: Transition):
    return str(subject.user_id)


def value_function(timestamp: int, subject: Subject, transition: Transition):
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
    factory = KafkaCallbackFactory(['localhost:9092'], 'doge-kafka-example')
    event_callback = factory.create('test_topic', key_function, value_function)

    datagen = DataOnlineGenerator(['offline', 'online', 'loan_screen'], 'offline', UserFactory(), 1, 60000, 1000)
    datagen.add_transition('income', 'offline', 'offline', 0.01, action_callback=income_callback, event_callback=event_callback)
    datagen.add_transition('spending', 'offline', 'offline', 0.1, action_callback=spending_callback, event_callback=event_callback)
    datagen.add_transition('login', 'offline', 'online', 0.1, event_callback=event_callback)
    datagen.add_transition('logout', 'online', 'offline', 70, event_callback=event_callback)
    datagen.add_transition('go_loan_screen', 'online', 'loan_screen', 30, event_callback=event_callback)
    datagen.add_transition('close_loan_screen', 'loan_screen', 'online', 40, event_callback=event_callback)
    datagen.add_transition('take_loan', 'loan_screen', 'online', 10, action_callback=take_loan_callback, event_callback=event_callback)

    datagen.start()
    factory.producer.flush()

from dataclasses import dataclass
from random import randrange

from doge_datagen import Transition, SubjectFactory, DataOnlineGenerator


@dataclass
class User:
    user_id: int
    balance: int
    loan_balance: int
    last_payment: int

    def __hash__(self):
        return self.user_id


class UserFactory(SubjectFactory[User]):
    def __init__(self):
        super().__init__()
        self.current_id = 0

    def create(self) -> User:
        user = User(self.current_id, randrange(0, 1000), 0, 0)
        self.current_id += 1
        return user


def income_callback(subject: User, transition: Transition):
    subject.balance += randrange(500, 2000)
    return True


def spending_callback(subject: User, transition: Transition):
    payment = randrange(10, 100)
    if payment > subject.balance:
        return False
    subject.balance -= payment
    subject.last_payment = payment
    return True


def take_loan_callback(subject: User, transition: Transition):
    subject.loan_balance += 10000
    subject.balance += 10000
    return True


def create_example_data_online_generator(sink):
    datagen = DataOnlineGenerator(['offline', 'online', 'loan_screen'], 'offline', UserFactory(), 10, 60000, 1000)
    datagen.add_transition('income', 'offline', 'offline', 0.01, action_callback=income_callback, event_sinks=[sink])
    datagen.add_transition('spending', 'offline', 'offline', 0.1, action_callback=spending_callback, event_sinks=[sink])
    datagen.add_transition('login', 'offline', 'online', 0.1, event_sinks=[sink])
    datagen.add_transition('logout', 'online', 'offline', 70, event_sinks=[sink])
    datagen.add_transition('open_loan_screen', 'online', 'loan_screen', 30, event_sinks=[sink])
    datagen.add_transition('close_loan_screen', 'loan_screen', 'online', 40, event_sinks=[sink])
    datagen.add_transition('take_loan', 'loan_screen', 'online', 10,
                           action_callback=take_loan_callback, event_sinks=[sink])
    return datagen

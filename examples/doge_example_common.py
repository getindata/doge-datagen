from random import randrange

from doge import Subject, Transition, SubjectFactory, DataOnlineGenerator


def income_callback(subject: Subject, transition: Transition):
    subject.balance += randrange(500,2000)
    return True


def spending_callback(subject: Subject, transition: Transition):
    payment = randrange(10, 100)
    if payment > subject.balance:
        return False
    subject.balance -= payment
    return True


def take_loan_callback(subject: Subject, transition: Transition):
    subject.loan_balance += 10000
    subject.balance += 10000
    return True


class UserFactory(SubjectFactory):
    def __init__(self):
        super().__init__()
        self.current_id = 0

    def create(self) -> Subject:
        user = SubjectFactory.create(self)
        user.user_id = self.current_id
        user.balance = randrange(0, 1000)
        user.loan_balance = 0
        self.current_id += 1
        return user


def create_example_data_online_generator(sink):
    datagen = DataOnlineGenerator(['offline', 'online', 'loan_screen'], 'offline', UserFactory(), 10, 60000, 1000)
    datagen.add_transition('income', 'offline', 'offline', 0.01, action_callback=income_callback, event_sink=sink)
    datagen.add_transition('spending', 'offline', 'offline', 0.1, action_callback=spending_callback, event_sink=sink)
    datagen.add_transition('login', 'offline', 'online', 0.1, event_sink=sink)
    datagen.add_transition('logout', 'online', 'offline', 70, event_sink=sink)
    datagen.add_transition('go_loan_screen', 'online', 'loan_screen', 30, event_sink=sink)
    datagen.add_transition('close_loan_screen', 'loan_screen', 'online', 40, event_sink=sink)
    datagen.add_transition('take_loan', 'loan_screen', 'online', 10, action_callback=take_loan_callback, event_sink=sink)
    return datagen

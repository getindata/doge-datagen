from random import randrange
from doge.doge import *


def printing_event_callback(timestamp: int, user: Subject, transition: Transition):
    print('[{}] User id: {}, balance: {}, loan_balance: {} made a transition {} from {} to {}'.format(timestamp, user.user_id, user.balance, user.loan_balance, transition.trigger, transition.from_state, transition.to_state))


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


if __name__ == '__main__':
    datagen = DataOnlineGenerator(['offline', 'online', 'loan_screen'], 'offline', UserFactory(), 1000, 60000, 24 * 60)
    datagen.add_transition('income', 'offline', 'offline', 0.01, action_callback=income_callback, event_callback=printing_event_callback)
    datagen.add_transition('spending', 'offline', 'offline', 0.1, action_callback=spending_callback, event_callback=printing_event_callback)
    datagen.add_transition('login', 'offline', 'online', 0.1, event_callback=printing_event_callback)
    datagen.add_transition('logout', 'online', 'offline', 70, event_callback=printing_event_callback)
    datagen.add_transition('go_loan_screen', 'online', 'loan_screen', 30, event_callback=printing_event_callback)
    datagen.add_transition('close_loan_screen', 'loan_screen', 'online', 40, event_callback=printing_event_callback)
    datagen.add_transition('take_loan', 'loan_screen', 'online', 10, action_callback=take_loan_callback, event_callback=printing_event_callback)

    datagen.start()

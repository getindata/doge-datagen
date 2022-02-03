import os
from typing import Dict, Any

from doge_datagen import Transition, DbSinkFactory
from examples.doge_example_common import create_example_data_online_generator, User


def row_mapper_function(timestamp: int, subject: User, transition: Transition) -> Dict[str, Any]:
    row = {
        'timestamp': timestamp,
        'user_id': subject.user_id,
        'balance': subject.balance,
        'loan_balance': subject.loan_balance,
        'event': transition.trigger
    }
    return row


if __name__ == '__main__':
    db_pass = os.getenv('PGPASSWORD', 'postgres')
    factory = DbSinkFactory('postgresql://postgres:{}@localhost:5432/postgres'.format(db_pass))
    sink = factory.create('events', row_mapper_function)

    datagen = create_example_data_online_generator(sink)

    datagen.start()

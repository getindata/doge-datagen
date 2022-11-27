import os
from typing import Dict, Any

from doge_datagen import Transition, DbSinkFactory
from examples.doge_example_common import create_example_data_online_generator, User
from sqlalchemy.dialects import oracle

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

    # Connecting to oracle data base
    oracle_dialect = oracle.dialect(max_identifier_length=30)

    url_mysql = URL('mysql',
      username='admin', password='admin',
      host='localhost', port='3306',
      database='testdb',
      query=oracle_dialect)

    # Connecting to PSG data base
    url_postgres = 'postgresql://postgres:{}@localhost:5432/postgres'.format(db_pass)

    # Replace specific url depending on your needs
    factory = DbSinkFactory(url_mysql)
    sink = factory.create('events', row_mapper_function)

    datagen = create_example_data_online_generator(sink)

    datagen.start()

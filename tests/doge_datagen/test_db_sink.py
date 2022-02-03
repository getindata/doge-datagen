from typing import Dict, Any
from unittest.mock import Mock

import pytest
import sqlalchemy
from testcontainers.postgres import PostgresContainer

from doge_datagen import DbSinkFactory, Subject, Transition


def row_mapper_function(timestamp: int, subject: Subject, transition: Transition) -> Dict[str, Any]:
    return {'some_column': 'some_value'}


@pytest.fixture
def postgres():
    with PostgresContainer() as postgres:
        yield postgres


@pytest.fixture
def engine(postgres):
    engine = sqlalchemy.create_engine(postgres.get_connection_url())
    engine.execute('create table some_table (some_column varchar(20))')
    yield engine


class TestDbSink:
        
    def test_db_sink(self, postgres, engine):
        factory = DbSinkFactory(postgres.get_connection_url())
        sink = factory.create('some_table', row_mapper_function)

        sink.collect(123, Mock(), Mock())
        sink.close()

        row = engine.execute("select * from some_table").fetchone()
        assert row['some_column'] == 'some_value'

    def test_table_exist_validation(self, postgres):
        factory = DbSinkFactory(postgres.get_connection_url())
        with pytest.raises(ValueError):
            factory.create('some_other_table', row_mapper_function)

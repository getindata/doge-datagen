from typing import Callable, Dict, Any, List

from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.engine import Engine

from doge_datagen import Subject, Transition
from doge_datagen import EventSink


class DbSink(EventSink):
    def __init__(self,
                 engine: Engine,
                 metadata: MetaData,
                 table_name: str,
                 row_mapper_function: Callable[[int, Subject, Transition], Dict[str, Any]],
                 batch_size: int):
        self.engine = engine
        self.table = Table(table_name, metadata)
        self.batch: List[Dict[str, Any]] = []
        self.row_mapper_function = row_mapper_function
        self.batch_size = batch_size
        self.__validate_table_exists()

    def collect(self, timestamp: int, subject: Subject, transition: 'Transition'):
        row = self.row_mapper_function(timestamp, subject, transition)
        self.batch.append(row)
        if len(self.batch) >= self.batch_size:
            self.__insert_batch()

    def close(self):
        if len(self.batch):
            self.__insert_batch()

    def __insert_batch(self):
        self.engine.execute(self.table.insert(), self.batch)
        self.batch = []

    def __validate_table_exists(self):
        if not self.table.exists(bind=self.engine):
            raise ValueError("Table {} does not exist".format(self.table.name))


class DbSinkFactory(object):
    batches: Dict[str, List[Dict[str, Any]]]
    tables: Dict[str, Table]

    def __init__(self, db_url: str):
        """
        :param db_url: database url in format acceptable by SQLAlchemy
        :type db_url: str
        """
        self.engine = create_engine(db_url)
        self.metadata = MetaData()
        self.metadata.reflect(self.engine)
        self.batches = {}
        self.tables = {}

    def create(self,
               table_name: str,
               row_mapper_function: Callable[[int, Subject, Transition], Dict[str, Any]],
               batch_size: int = 1000) -> DbSink:
        """
        :param table_name: name of the table that will store events
        :type table_name: str
        :param row_mapper_function: function that will map timestamp, subject and transition instance to database row
        :type row_mapper_function: Callable[[int, Subject, Transition], Dict[str, Any]]
        :param batch_size: size of a batch that will be inserted to database
        :type batch_size: int
        :return: DbSink instance
        :rtype: DbSink
        """
        return DbSink(self.engine, self.metadata, table_name, row_mapper_function, batch_size)

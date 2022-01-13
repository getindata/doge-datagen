from typing import Callable, Dict, Any, List

from sqlalchemy import create_engine, MetaData, Table

from doge import Subject, Transition


class DbSinkFactory(object):
    batches: Dict[str, List[Dict[str, Any]]]
    tables: Dict[str, Table]

    def __init__(self,
                 db_url: str,
                 batch_size: int = 1000):
        self.engine = create_engine(db_url)
        self.metadata = MetaData()
        self.metadata.reflect(self.engine)
        self.batches = {}
        self.batch_size = batch_size
        self.tables = {}

    def create(self, table_name: str, row_mapper_function: Callable[[int, Subject, Transition], Dict[str, Any]]):
        table = Table(table_name, self.metadata)
        self.tables[table_name] = table
        self.batches[table_name] = []

        def callback(timestamp: int, subject: Subject, transition: Transition):
            row = row_mapper_function(timestamp, subject, transition)
            self.batches[table_name].append(row)
            if len(self.batches[table_name]) >= self.batch_size:
                self.__insert_batch(table_name)
        return callback

    def __insert_batch(self, table_name: str):
        batch = self.batches[table_name]
        self.engine.execute(self.tables[table_name].insert(), batch)
        self.batches[table_name] = []

    def flush(self):
        for table_name, batch in self.batches.items():
            if len(batch) > 0:
                self.__insert_batch(table_name)

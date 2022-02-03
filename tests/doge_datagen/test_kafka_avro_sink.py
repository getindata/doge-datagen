from typing import Dict, Any
from unittest.mock import Mock

import pytest
from confluent_kafka import DeserializingConsumer
from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from testcontainers.kafka import KafkaContainer

from doge_datagen import KafkaAvroSinkFactory, Subject, Transition
from tests.testcontainers.schemaregistry import SchemaRegistryContainer

TEST_TOPIC = 'test-topic'
KEY_SCHEMA = """
{
  "namespace": "com.getindata.doge",
  "type": "record",
  "name": "SomeKey",
  "fields": [
    { "name":  "key", "type":  "string"}
  ]
}
"""
VALUE_SCHEMA = """
{
  "namespace": "com.getindata.doge",
  "type": "record",
  "name": "SomeValue",
  "fields": [
    { "name":  "value", "type":  "string"}
  ]
}
"""


def key_function(subject: Subject, transition: Transition) -> Dict[str, Any]:
    return {'key': 'some_key'}


def value_function(timestamp: int, subject: Subject, transition: Transition) -> Dict[str, Any]:
    return {'value': 'some_value'}


@pytest.fixture
def kafka():
    with KafkaContainer() as kafka:
        yield kafka


@pytest.fixture
def schema_registry(kafka):
    with SchemaRegistryContainer.with_kafka_container(kafka) as schema_registry:
        yield schema_registry


class TestKafkaAvroSink:
    def test_kafka_avro_sink(self, kafka, schema_registry):
        self.__create_test_topic(kafka.get_bootstrap_server())
        self.__test_kafka_avro_sink(kafka.get_bootstrap_server(), schema_registry.get_url())

    def __test_kafka_avro_sink(self, kafka_bootstrap, schema_registry_url):
        factory = KafkaAvroSinkFactory([kafka_bootstrap], schema_registry_url, 'test-client')
        sink = factory.create(TEST_TOPIC, key_function, KEY_SCHEMA, value_function, VALUE_SCHEMA)

        sink.collect(123, Mock(), Mock())
        sink.close()

        consumer = self.__create_consumer(kafka_bootstrap, schema_registry_url)
        consumer.subscribe([TEST_TOPIC])
        msg = consumer.poll(10)
        assert msg.key()['key'] == 'some_key'
        assert msg.value()['value'] == 'some_value'

    @staticmethod
    def __create_consumer(kafka_bootstrap, schema_registry_url):
        deserializer = AvroDeserializer(SchemaRegistryClient({'url': schema_registry_url}))
        consumer = DeserializingConsumer({
            'bootstrap.servers': kafka_bootstrap,
            'group.id': 'test-group',
            'auto.offset.reset': 'earliest',
            'key.deserializer': deserializer,
            'value.deserializer': deserializer})
        return consumer

    @staticmethod
    def __create_test_topic(kafka_bootstrap):
        admin_client = AdminClient({"bootstrap.servers": kafka_bootstrap})
        admin_client.create_topics([NewTopic(TEST_TOPIC, 1, 1)])[TEST_TOPIC].result(10)

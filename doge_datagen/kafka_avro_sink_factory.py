from typing import Iterable, Callable, Any, Dict

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

from doge_datagen import Subject, Transition, KafkaSinkFactory, KafkaSink


class KafkaAvroSinkFactory(object):

    def __init__(self,
                 conf: dict,
                 schema_registry_url: str,
                 buffer_size=100000):
        """
        :pram conf : dictionary with configuration parameters specific for cloud providers
        :type conf: dictionary
        :param schema_registry_url: schema registry url for example http://localhost:8081
        :type schema_registry_url: str
        """
        
        self.factory = KafkaSinkFactory(conf, buffer_size)
        schema_registry_conf = {'url': schema_registry_url}
        self.schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    def create(self, topic: str,
               key_function: Callable[[Subject, Transition], Dict[str, Any]],
               key_schema: str,
               value_function: Callable[[int, Subject, Transition], Dict[str, Any]],
               value_schema: str) -> KafkaSink:
        """
        :param topic: topic name to which events will be emitted
        :type topic: str
        :param key_function: function that converts subject and transition to a format consumable by key serializer
        :type key_function: Callable[[Subject, Transition], K]
        :param key_schema: Avro compliant schema for key serialization
        :type key_schema: str
        :param value_function: function that converts timestamp, subject and transition to a format consumable by
            value serializer
        :type value_function: Callable[[int, Subject, Transition], V]
        :param value_schema: Avro compliant schema for value serialization
        :type value_schema: str
        :return: KafkaSink instance
        :rtype: KafkaSink
        """
        key_serializer = AvroSerializer(self.schema_registry_client, key_schema)
        value_serializer = AvroSerializer(self.schema_registry_client, value_schema)
        return self.factory.create(topic, key_function, value_function, key_serializer, value_serializer)

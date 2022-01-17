from typing import Iterable, Callable, TypeVar

from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, StringSerializer, Serializer, MessageField

from doge import Subject, Transition, EventSink

K = TypeVar('K')
V = TypeVar('V')


class KafkaSink(EventSink):
    def __init__(self,
                 producer: Producer,
                 topic: str,
                 key_function: Callable[[Subject, Transition], K],
                 key_serializer: Serializer,
                 value_function: Callable[[int, Subject, Transition], V],
                 value_serializer: Serializer):
        self.producer = producer
        self.topic = topic
        self.key_function = key_function
        self.key_serializer = key_serializer
        self.value_function = value_function
        self.value_serializer = value_serializer

    def collect(self, timestamp: int, subject: Subject, transition: 'Transition'):
        key_ctx = SerializationContext(self.topic, MessageField.KEY)
        key = self.key_serializer(self.key_function(subject, transition), key_ctx)
        value_ctx = SerializationContext(self.topic, MessageField.VALUE)
        value = self.value_serializer(self.value_function(timestamp, subject, transition), value_ctx)
        self.producer.produce(self.topic,
                              key=key,
                              value=value,
                              timestamp=timestamp)

    def close(self):
        self.producer.flush()


class KafkaSinkFactory(object):
    def __init__(self,
                 bootstrap_servers: Iterable[str],
                 client_id: str):
        """
        :param bootstrap_servers: list of bootstrap servers
        :type bootstrap_servers: Iterable[str]
        :param client_id: sink client id
        :type client_id: str
        """
        conf = {
            'bootstrap.servers': ','.join(bootstrap_servers),
            'client.id': client_id,
        }
        self.producer = Producer(conf)

    def create(self, topic: str,
               key_function: Callable[[Subject, Transition], K],
               value_function: Callable[[int, Subject, Transition], V],
               key_serializer=StringSerializer(),
               value_serializer=StringSerializer()) -> KafkaSink:
        """
        :param topic: topic name to which events will be emitted
        :type topic: str
        :param key_function: function that converts subject and transition to a format consumable by key serializer
        :type key_function: Callable[[Subject, Transition], K]
        :param value_function: function that converts timestamp, subject and transition to a format consumable by
            value serializer
        :type value_function: Callable[[int, Subject, Transition], V]
        :param key_serializer: Serializer instance that will be used to serialize key_function output into bytes
        :type key_serializer: Serializer
        :param value_serializer: Serializer instance that will be used to serialize value_function output into bytes
        :type value_serializer: Serializer
        :return: KafkaSink instance
        :rtype: KafkaSink
        """
        return KafkaSink(self.producer, topic, key_function, key_serializer, value_function, value_serializer)

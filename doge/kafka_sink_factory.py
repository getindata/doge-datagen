from typing import Iterable, Callable, TypeVar

from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import SerializationContext, StringSerializer

from doge import Subject, Transition, EventSink

K = TypeVar('K')
V = TypeVar('V')


class KafkaSink(EventSink):

    def __init__(self,
                 producer: SerializingProducer,
                 topic: str,
                 key_function: Callable[[Subject, Transition], K],
                 value_function: Callable[[int, Subject, Transition], V]):
        self.producer = producer
        self.topic = topic
        self.key_function = key_function
        self.value_function = value_function

    def collect(self, timestamp: int, subject: Subject, transition: 'Transition'):
        self.producer.produce(self.topic,
                              key=self.key_function(subject, transition),
                              value=self.value_function(timestamp, subject, transition),
                              timestamp=timestamp)

    def close(self):
        self.producer.flush()


class KafkaSinkFactory(object):
    def __init__(self,
                 bootstrap_servers: Iterable[str],
                 client_id: str,
                 key_serializer: Callable[[K, SerializationContext], bytes] = StringSerializer(),
                 value_serializer: Callable[[V, SerializationContext], bytes] = StringSerializer()):
        """
        :param bootstrap_servers: list of bootstrap servers
        :type bootstrap_servers: Iterable[str]
        :param client_id: sink client id
        :type client_id: str
        :param key_serializer: serializer that will be used to serialize a key to bytes
        :type key_serializer: Callable[[K, SerializationContext], bytes]
        :param value_serializer: serializer that will be used to serialize a value to bytes
        :type value_serializer: Callable[[V, SerializationContext], bytes]
        """
        conf = {
            'bootstrap.servers': ','.join(bootstrap_servers),
            'client.id': client_id,
            'key.serializer': key_serializer,
            'value.serializer': value_serializer
        }
        self.producer = SerializingProducer(conf)

    def create(self, topic: str,
               key_function: Callable[[Subject, Transition], K],
               value_function: Callable[[int, Subject, Transition], V]) -> KafkaSink:
        """
        :param topic: topic name to which events will be emitted
        :type topic: str
        :param key_function: function that converts subject and transition to a format consumable by key serializer
        :type key_function: Callable[[Subject, Transition], K]
        :param value_function: function that converts timestamp, subject and transition to a format consumable by
            value serializer
        :type value_function: Callable[[int, Subject, Transition], V]
        :return: KafkaSink instance
        :rtype: KafkaSink
        """
        return KafkaSink(self.producer, topic, key_function, value_function)

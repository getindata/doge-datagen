from typing import Iterable, Callable, TypeVar

from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import SerializationContext, StringSerializer

from doge import Subject, Transition

K = TypeVar('K')
V = TypeVar('V')


class KafkaSinkFactory(object):
    def __init__(self,
                 bootstrap_servers: Iterable[str],
                 client_id: str,
                 key_serializer: Callable[[K, SerializationContext], bytes] = StringSerializer(),
                 value_serializer: Callable[[V, SerializationContext], bytes] = StringSerializer()):
        conf = {
            'bootstrap.servers': ','.join(bootstrap_servers),
            'client.id': client_id,
            'key.serializer': key_serializer,
            'value.serializer': value_serializer
        }
        self.producer = SerializingProducer(conf)

    def create(self, topic: str, key_function: Callable[[Subject, Transition], K], value_function: Callable[[int, Subject, Transition], V]):
        def callback(timestamp: int, subject: Subject, transition: Transition):
            self.producer.produce(topic, key=key_function(subject, transition), value=value_function(timestamp, subject, transition), timestamp=timestamp)
        return callback

    def flush(self):
        self.producer.flush()
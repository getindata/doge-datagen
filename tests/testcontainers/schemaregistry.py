import urllib

from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_container_is_ready
from testcontainers.kafka import KafkaContainer


class SchemaRegistryContainer(DockerContainer):
    def __init__(self, kafka_bootstrap_servers, image='confluentinc/cp-schema-registry:7.0.1', port_to_expose=8081, ):
        super(SchemaRegistryContainer, self).__init__(image)
        self.port_to_expose = port_to_expose
        self.with_exposed_ports(self.port_to_expose)
        self.with_env('SCHEMA_REGISTRY_HOST_NAME', 'schema-registry')
        self.with_env('SCHEMA_REGISTRY_LISTENERS', 'http://0.0.0.0:8081')
        self.with_env('SCHEMA_REGISTRY_LOG4J_ROOT_LOGLEVEL', 'INFO')
        self.with_env('SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS', kafka_bootstrap_servers)

    @staticmethod
    def with_kafka_container(kafka: KafkaContainer):
        bootstrap = kafka.exec('hostname -i').output.decode('utf-8').strip() + ":9092"
        return SchemaRegistryContainer(bootstrap)

    @wait_container_is_ready()
    def _connect(self):
        res = urllib.request.urlopen(self.get_url() + '/subjects')
        if res.status != 200:
            raise Exception()

    def get_url(self):
        host = self.get_container_host_ip()
        port = self.get_exposed_port(self.port_to_expose)
        return 'http://{}:{}'.format(host, port)

    def start(self):
        super().start()
        self._connect()
        return self

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer


class ProducerAvro(object):
    def __init__(self):
        self.topic = "avro-topic"
        self.schema_str = """
            {
              "type": "record",
              "name": "User",
              "fields": [
                {"name": "id", "type": "string"},
                {"name": "name", "type": "string"},
                {"name": "age", "type": ["null", "int"], "default": null}
              ]
            }
        """
        self.schema_registry_client = SchemaRegistryClient({
            "url": "http://192.168.114.31:8889"
        })
        self.avro_serializer = AvroSerializer(
            self.schema_registry_client,
            self.schema_str,
        )
        self.producer = SerializingProducer({
            "bootstrap.servers": "kafka.114.31",
            "value.serializer": self.avro_serializer,
        })

    def delivery_report(self, err, message):
        if err is not None:
            print(f"[!][!] Delivery failed: {err}")
        else:
            print(f"[^][^] Delivered to {message.topic()}")

    def produce(self, message):
        self.producer.produce(
            topic=self.topic,
            key=message.get('id'),
            value=message,
            on_delivery=self.delivery_report,
        )
        self.producer.flush()


if __name__ == '__main__':
    producer = ProducerAvro()
    producer.produce(
        {
            "id": "user-3",
            "name": "suguru",
            "age": 21,
        }
    )


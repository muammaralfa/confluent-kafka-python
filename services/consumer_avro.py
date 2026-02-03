from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
import json


class KafkaConsumerAvro:
    def __init__(self):
        self.topic = "avro-topic"
        self.schema_registry_conf = {
            "url": "http://192.168.114.31:8889"
        }
        self.schema_registry_client = SchemaRegistryClient(self.schema_registry_conf)
        self.conf = {
            "bootstrap.servers": "kafka.114.31",
            "group.id": "avro-consumer-group",
            "auto.offset.reset": "earliest",
            "key.deserializer": StringDeserializer("utf_8"),
            "value.deserializer": AvroDeserializer(self.schema_registry_client),
        }
        self.consumer = DeserializingConsumer(self.conf)

    def consume(self):
        self.consumer.subscribe([self.topic])
        print("[^][^] Consuming messages from topic '{}'".format(self.topic))
        try:
            while True:
                message = self.consumer.poll(timeout=1.0)
                if message is None:
                    continue
                if message.error():
                    raise KafkaException(message.error())

                key = message.key()
                value = message.value() # <-- already deserialized dict!
                print(key, value)
                print(type(key), type(value))

                self.consumer.commit()

        except KeyboardInterrupt:
            pass

        finally:
            self.consumer.close()
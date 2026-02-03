from confluent_kafka import Consumer, KafkaException
import json


class kafkaConsumer:
    def __init__(self):
        self.conf = {
            "bootstrap.servers": "kafka.114.31",
            "group.id": "python-consumer-group",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False  # recommended for better control
        }
        self.topic = "confluent-topic"
        self.consumer = Consumer(self.conf)

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

                key = message.key().decode("utf-8") if message.key() else None
                value = json.loads(message.value().decode("utf-8"))
                print(key, value)

                self.consumer.commit()

        except KeyboardInterrupt:
            pass

        finally:
            self.consumer.close()
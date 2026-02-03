from confluent_kafka import Producer
import json


class ProducerKafka:
    def __init__(self):
        self.conf = {
            "bootstrap.servers": "kafka.114.31"
        }
        self.producer = Producer(self.conf)
        self.topic = 'confluent-topic'

    def delivery_report(self, err, message):
        if err is not None:
            print(f"[!][!] Delivery failed: {err}")
        else:
            print(f"[^][^] Delivered to {message.topic()}")

    def produce(self, message):
        self.producer.produce(
            topic=self.topic,
            value=json.dumps(message).encode("utf-8"),
            on_delivery=self.delivery_report,
        )
        self.producer.flush()


if __name__ == '__main__':
    producer = ProducerKafka()
    message = {
        "pic": "jotaro",
        "data": {
            "nama": "kujo",
            "divisi": "tim pukul",
            "korban": 102,
            "rank": "S"
        }
    }

    producer.produce(message)
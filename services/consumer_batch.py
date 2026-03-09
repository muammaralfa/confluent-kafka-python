from confluent_kafka import Consumer, KafkaException
import json


class kafkaConsumer:
    def __init__(self):
        self.topic = "confluent-topic"
        self.consumer = Consumer({
            "bootstrap.servers": "kafka.114.31",
            "group.id": "python-consumer-group",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False  # recommended for better control
        })

    def consume(self):
        """
            consumer.consume() = fetches multiple messages in a batch.
            - Fetches up to N messages.
            - Waits up to timeout if no messages available.
            - Returns immediately if messages exist.

            When to use
            - Event-driven processing
            - high-volume consumers
            - batch logic
        """
        
        self.consumer.subscribe([self.topic])
        print("[^][^] Consuming messages from topic '{}'".format(self.topic))
        try:
            while True:
                messages = self.consumer.consume(100, 10.0)
                print(f"messages count:  {len(messages)}")
                if messages is None:
                    continue
                
                actions = []
                for message in messages:
                    key = message.key().decode("utf-8") if message.key() else None
                    value = json.loads(message.value().decode("utf-8"))
                    actions.append(value)
                    
                if actions:
                    actions.clear()

                self.consumer.commit()

        except KeyboardInterrupt:
            pass

        finally:
            self.consumer.close()
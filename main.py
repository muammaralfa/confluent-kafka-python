from services.consumer import kafkaConsumer
from services.consumer_avro import KafkaConsumerAvro

class Main:
    def __init__(self):
        self.consumer = kafkaConsumer()
        self.consumer_avro = KafkaConsumerAvro()

    def run(self):
        self.consumer.consume()

    def run_avro(self):
        self.consumer_avro.consume()

if __name__ == '__main__':
    main = Main()
    main.run_avro()
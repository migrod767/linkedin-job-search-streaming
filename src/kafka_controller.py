from kafka import KafkaProducer
import time


class KafkaProducer:
    def __init__(self):
        self.kafka_server = 'localhost:9092'
        self.kafka_topic = 'linkedin_scrapper'
        self.kafka_producer = KafkaProducer(bootstrap_servers=self.kafka_server)

    def producer_send_message(self, msg: str):
        """
        Produce send messages into Kafka
        :param msg: Message to be sent
        :return:
        """
        byte_msg = bytes(msg, 'utf-8')
        self.kafka_producer.send('linkedin_scrapper', byte_msg) \
            .add_callback(self.success) \
            .add_errback(self.error)
        self.kafka_producer.flush()

    def success(self, metadata):
        """
        Prints the message when it has been successful
        :param metadata:
        :return:
        """
        print(metadata.topic)

    def error(self, exception):
        """
        Prints the message when and error occurred
        :param exception:
        :return:
        """
        print(exception)


if __name__ == '__main__':
    kafka_c = KafkaProducer()

    messages = ['{message: This is a test}',
                '"type": "search", "file": "file_bla_bla.json", "info": "more info bla bla"',
                'key: value',
                '"type":"job"']

    for message in messages:
        print(f"Sending {message}")
        kafka_c.producer_send_message(message)
        time.sleep(2)



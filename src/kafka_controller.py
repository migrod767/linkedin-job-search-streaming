from kafka import KafkaProducer
import time


class KafkaController:
    def __init__(self):
        self.kafka_server = 'localhost:9092'
        self.kafka_topic = 'linkedin_scrapper'
        self.kafka_producer = KafkaProducer(bootstrap_servers=self.kafka_server)

    def producer_send_message(self, msg:str):
        # Function to send messages into Kafka
        byte_msg = bytes(msg, 'utf-8')
        self.kafka_producer.send('linkedin_scrapper', byte_msg)\
            .add_callback(self.success)\
            .add_errback(self.error)
        self.kafka_producer.flush()

    def success(self, metadata):
        print(metadata.topic)

    def error(self, exception):
        print(exception)


if __name__ == '__main__':
    kafka_c = KafkaController()

    messages = ['{message: This is a test}',
                '"type": "search", "file": "file_bla_bla.json", "info": "more info bla bla"',
                'key: value',
                '"type":"job"']

    for message in messages:
        print(f"Sending {message}")
        kafka_c.producer_send_message(message)
        time.sleep(2)


import configparser
import os


class Credentials:
    def __init__(self):
        self.credentials = configparser.ConfigParser()

    def get_credentials_path(self):
        try:
            creds_path = os.getenv('CREDENTIALS_PATH')
        except:
            raise Exception("No CREDENTIALS_PATH available")

        return creds_path

    def load_credentials(self, credentials_path=None):
        if credentials_path is None:
            credentials_path = self.get_credentials_path()

        self.credentials.read_file(open(credentials_path))

    def get_kafka_bootstrap_servers(self):
        return self.credentials['Kafka']['kafka_bootstrap_servers']

    def get_kafka_topic_job_data(self):
        return self.credentials['Kafka']['kafka_topic_job_data']

    def get_kafka_topic_search_data(self):
        return self.credentials['Kafka']['kafka_topic_search_data']

    def get_aws_rds_endpoint(self):
        return self.credentials['AWS']['rds_endpoint']

    def get_aws_rds_user(self):
        return self.credentials['AWS']['rds_user']

    def get_aws_rds_pass(self):
        return self.credentials['AWS']['rds_pass']

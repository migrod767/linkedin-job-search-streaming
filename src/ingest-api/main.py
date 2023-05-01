import json
import os

import uvicorn
from fastapi import FastAPI
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from kafka import KafkaProducer
from pydantic import BaseModel

import credentials_controller


class JobData(BaseModel):
    SearchId: str
    PageNumber: int
    JobId: str
    ScrapeDt: str
    Title: str
    Company: str
    Location: str
    JobUrl: str
    ScrapTime: str
    Description: str
    SeniorityLevel: str
    EmploymentType: str
    JobFunction: str
    Industries: str
    JobPostedDt: str


class SearchData(BaseModel):
    SearchId: str
    SearchUrl: str
    SearchDt: str
    Keywords: str
    Location: str
    GeoId: int
    PostedDateFilter: str
    OnSiteRemote: str


class GeneralData(BaseModel):
    Message: str


# This is important for general execution and the docker later
app = FastAPI()

# Global Variables
os.environ['CREDENTIALS_PATH'] = '../../credentials.conf'
creds_c = credentials_controller.Credentials()
creds_c.load_credentials()


# Base URL
@app.get("/")
async def root():
    return {"Message": "Server is up"}


@app.post("/ingest/search")
async def post_search_data(item: GeneralData):  # body awaits a json with invoice item information
    try:
        json_as_string = dict(item)['Message']
        kafka_produce_send_msg(json_as_string, topic=creds_c.get_kafka_topic_search_data())

        return JSONResponse(content=dict(item), status_code=201)

    except ValueError:
        return JSONResponse(content=jsonable_encoder(dict(item)), status_code=400)


@app.post("/ingest/job")
async def post_job_data(item: GeneralData):  # body awaits a json with invoice item information
    try:
        json_as_string = dict(item)['Message']
        kafka_produce_send_msg(json_as_string, topic=creds_c.get_kafka_topic_job_data())

        return JSONResponse(content=dict(item), status_code=201)

    except ValueError:
        return JSONResponse(content=jsonable_encoder(dict(item)), status_code=400)


def kafka_produce_send_msg(outbound_message, topic):
    producer = KafkaProducer(bootstrap_servers=creds_c.get_kafka_bootstrap_servers())
    # Write the string as bytes because Kafka needs it this way
    producer.send(topic, value=bytes(outbound_message, 'utf-8'))
    producer.flush()


if __name__ == "__main__":
    uvicorn.run("main:app", log_level="info")




# Add endpoint to check kafka
# import kafka
#
# consumer = kafka.KafkaConsumer(group_id='test', bootstrap_servers=['localhost:9092'])
# topics = consumer.topics()
#
# if not topics:
#     raise RuntimeError()
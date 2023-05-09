# LinkedIn Job Scrapper

## Project Description

The aim of this project is to develop a robust and efficient data pipeline for collecting and analyzing job postings
from LinkedIn. The pipeline is built as a real-time streaming system, leveraging cutting-edge technologies such as
FastAPI, Kafka, Spark, PostgresSQL, and Streamlit.

FastAPI is used as the client API for the pipeline, providing a highly performant and scalable interface for data
ingestion. Kafka is employed as the message queue to enable seamless communication between different components of the
pipeline. Spark serves as the data processing framework, enabling the pipeline to handle large volumes of data with
ease. PostgresSQL is used for data storage, providing a reliable and secure repository for the scraped job postings.

In addition to these core components, Streamlit is utilized for data visualization and keyword searching, making it easy
to explore and extract insights from the collected data. Overall, this pipeline provides a powerful, scalable and
flexible solution for gathering, processing, and analyzing job postings from LinkedIn, opening up a world of
possibilities for data-driven decision-making.

## Data Flow Architecture

![Alt text](StreamArquitechtureLinkedIn.png?raw=true "Data Flow Architecture")


## Stack

* Python
* FastAPI - API
* Kafka - Message Queue
* Spark - Data Processing
* PostgresSQL (AWS RDS) - Data Storage
* StreamLite - Visualization


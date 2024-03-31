from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from json import loads
import requests
from utils import logger
from sqlalchemy.orm import Session
from uuid import uuid4
from models import Reports 
from config import db_connection, schema_registry_url, bootstrap_server
from utils import get_active_reports, send_report_to_queue, flush_producer, get_metastore_engine  
from confluent_kafka.serialization import (
    StringSerializer,
    SerializationContext,
    MessageField,
)


encoder = StringSerializer()

class User(object):
    def __init__(self, report_name, sql_query, db_connection, create_zip_file,
                 mail_to, mail_cc, mail_bcc, mail_subject, mail_body):
        self.report_name = report_name
        self.sql_query = sql_query
        self.db_connection = db_connection
        self.create_zip_file = create_zip_file
        self.mail_to = mail_to
        self.mail_cc = mail_cc
        self.mail_bcc = mail_bcc
        self.mail_subject = mail_subject
        self.mail_body = mail_body


def delivery_report(err, msg):
    if err is not None:
        logger.info("Delivery failed for User record {}: {}".format(msg.key(), err))
        pass
    else:    
        logger.info("Delivery Sucessfully for User record {}: {}".format(msg.key(), err))
        # logger.info('User record {} successfully produced to {} [{}] at offset {}'.format(
        # msg.key(), msg.topic(), msg.partition(), msg.offset()))



def user_to_dict(user, ctx):
    return {
        "report_name": user.report_name,
        "sql_query": user.sql_query,
        "db_connection": user.db_connection,
        "create_zip_file": user.create_zip_file,
        "mail_to": user.mail_to,
        "mail_cc": user.mail_cc,
        "mail_bcc": user.mail_bcc,
        "mail_subject": user.mail_subject,
        "mail_body": user.mail_body
    }


def get_schema():
    schema_str = """
        {
          "type": "record",
          "name": "User",
          "fields": [
            {"name": "report_name", "type": "string"},
            {"name": "sql_query", "type": "string"},
            {"name": "db_connection", "type": "string"},
            {"name": "create_zip_file", "type": "boolean"},
            {"name": "mail_to", "type": "string"},
            {"name": "mail_cc", "type": "string"},
            {"name": "mail_bcc", "type": "string"},
            {"name": "mail_subject", "type": "string"},
            {"name": "mail_body", "type": "string"}
          ]
        }
    """
    return schema_str


def main():
    schema = get_schema()
    topic = "report5"
    bootstrap_servers = bootstrap_server
    schema_registry_conf = {'url': schema_registry_url}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    avro_serializer = AvroSerializer(schema_registry_client, schema, user_to_dict)
    producer_conf = {'bootstrap.servers': bootstrap_servers, 'client.id': 'producer_1'}
    producer = Producer(producer_conf)

    logger.info("Producing user records to topic {}. ^C to exit.".format(topic))

    try:
        engine = get_metastore_engine()
        with Session(engine) as session_metastore:
            reports_metadata = get_active_reports(session_metastore, schedule='DAILY', schedule_type='DAILY_AMS')
            for report in reports_metadata: 
                report_key = str(report.report_id) + "-" + report.query_type
                print("Report Key:", report_key)
                print("report:", report)
               
                # report_name = input("Provide report_name: ")
                # sql_query = input("Provide sql_query: ")
                # db_connection = input("provide db_connection:")
                # create_zip_file = bool(input("provide create_zip_file:"))
                # mail_to = input("provide mail_to:")
                # mail_cc = input("provide mail_cc:")
                # mail_bcc = input("provide mail_bcc:")
                # mail_subject = input("provide mail_subject:")
                # mail_body = input("provide mail_body:")
                # user = User(report_name, sql_query, db_connection, create_zip_file,
                #         mail_to, mail_cc, mail_bcc, mail_subject, mail_body)
                user = User(report.report_name, report.sql_query, report.db_connection, report.create_zip_file,
                                           report.mail_to, report.mail_cc, report.mail_bcc, report.mail_subject, report.mail_body)
                # print(user)


                key = encoder(report_key)
                value = avro_serializer(user, SerializationContext(topic, MessageField.VALUE))
    
                producer.produce(topic=topic,
                                key=key,
                                value=value,
                                on_delivery=delivery_report)
                # Print the key and value after encoding
                print("Encoded Key:", key)
                print("avro_serializer Value:", value)
                send_report_to_queue(producer,key,value)
                


    except KeyboardInterrupt:
        pass
    except Exception as e:
       logger.exception("An error occurred: {}".format(str(e)))
    
    logger.info("\nFlushing records...")
    flush_producer(producer)


if __name__ == "__main__":
    main()



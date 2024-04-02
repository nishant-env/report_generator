from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from json import loads
import requests
from utils import logger
from sqlalchemy.orm import Session
from uuid import uuid4
from models import Reports 
from config import db_connection, schema_registry_url
from utils import get_active_reports, send_report_to_queue, flush_producer, get_metastore_engine, get_schema, User, user_to_dict, producer_conf
from confluent_kafka.serialization import (
    StringSerializer,
    SerializationContext,
    MessageField,
)


encoder = StringSerializer()

def main():
    schema = get_schema()
    topic = "esewa3"
    schema_registry_conf = {'url': schema_registry_url}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    avro_serializer = AvroSerializer(schema_registry_client, schema, user_to_dict)
    producer = Producer(producer_conf)

    logger.info("Producing user records to topic {}. ^C to exit.".format(topic))

    try:
        engine = get_metastore_engine()
        with Session(engine) as session_metastore:
            reports_metadata = get_active_reports(session_metastore, schedule='DAILY', schedule_type='DAILY_AMS')
            for report in reports_metadata: 
                report_key = str(report.report_id) + "-" + report.query_type
                
                user = User(report.report_name, report.sql_query, report.db_connection, report.create_zip_file,
                                           report.mail_to, report.mail_cc, report.mail_bcc, report.mail_subject, report.mail_body)
             


                key = encoder(report_key)
                value = avro_serializer(user, SerializationContext(topic, MessageField.VALUE))
                send_report_to_queue(producer,key,value)
    except KeyboardInterrupt:
        pass
    except Exception as e:
       logger.exception("An error occurred: {}".format(str(e)))

    flush_producer(producer)


if __name__ == "__main__":
    main()



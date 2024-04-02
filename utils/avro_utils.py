
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
from config import schema_registry_url, kafka_topic
from models import avro_schema_str



class ReportModel(object):
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



def report_obj_to_dict(report, ctx):
    return {
        "report_name": report.report_name,
        "sql_query": report.sql_query,
        "db_connection": report.db_connection,
        "create_zip_file": report.create_zip_file,
        "mail_to": report.mail_to,
        "mail_cc": report.mail_cc,
        "mail_bcc": report.mail_bcc,
        "mail_subject": report.mail_subject,
        "mail_body": report.mail_body
    }

def dict_to_report_obj(obj, ctx):
    if obj is None:
        return None

    return ReportModel(report_name=obj['report_name'], sql_query=obj['sql_query'], db_connection=obj['db_connection'],
                create_zip_file=obj['create_zip_file'], mail_to=obj['mail_to'], mail_cc=obj['mail_cc'],
                mail_bcc=obj['mail_bcc'], mail_subject=obj['mail_subject'], mail_body=obj['mail_body'])




# schema registry config
schema_registry_config = {'url': schema_registry_url}
schema_registry_client = SchemaRegistryClient(schema_registry_config)


## Serializer definition
avro_serializer = AvroSerializer(schema_registry_client, avro_schema_str, report_obj_to_dict)
def avro_serialization_formatter(report_obj):
    report_model = ReportModel(
           report_obj["report_name"],
           report_obj["sql_query"],
           report_obj["db_connection"],
           report_obj["create_zip_file"],
            report_obj["mail_to"],
            report_obj["mail_cc"],
            report_obj["mail_bcc"],
            report_obj["mail_subject"],
           report_obj["mail_body"]
    )
    serialized_object = avro_serializer(report_model, SerializationContext(kafka_topic, MessageField.VALUE))
    return serialized_object

### deserialization
avro_deserializer = AvroDeserializer(schema_registry_client, avro_schema_str, dict_to_report_obj)
def avro_deserialization_formatter(serialized_obj):
    return avro_deserializer(serialized_obj, SerializationContext(kafka_topic, MessageField.VALUE))
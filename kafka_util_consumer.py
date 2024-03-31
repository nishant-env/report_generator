import os
import json
from config import schema_registry_url,bootstrap_server
from utils import logger, generate_report_file, send_email, update_sent
from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.serialization import StringDeserializer

decoder = StringDeserializer(codec='utf_8')

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

def user_to_dict(obj, ctx):
    if obj is None:
        return None

    return User(report_name=obj['report_name'], sql_query=obj['sql_query'], db_connection=obj['db_connection'],
                create_zip_file=obj['create_zip_file'], mail_to=obj['mail_to'], mail_cc=obj['mail_cc'],
                mail_bcc=obj['mail_bcc'], mail_subject=obj['mail_subject'], mail_body=obj['mail_body'])

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
    
    avro_deserializer = AvroDeserializer(schema_registry_client, schema, user_to_dict)
    
    consumer_conf = {'bootstrap.servers': bootstrap_servers,
                     'group.id': "group_1_1",
                     'auto.offset.reset': "earliest"} 

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic]) 
    logger.info('Subscribed to the topic')

    try:
        while True:
            logger.info("Polling")
            msg = consumer.poll(timeout=3.0)
            if msg is None:
                print("No messages")
                continue
            
            if msg:
                if not msg.error():
                    print(msg.value())
                    print(msg.key())
                    print(msg.partition())
                    print(msg.offset())
                    logger.info(msg)


            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.info('End of partition')
                    continue
                else:
                    raise KafkaException(msg.error())

            # report_id = decoder(msg.key).split('-')[0] 
            report_id = msg.key().decode('utf-8').split('-')[0]
            user = avro_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
            print("Decoded_report_id:", report_id)
            print("Decoded_user:", user)
            
            if user is not None:
                logger.info("Received User record {}: report_name: {}\n"
                            "\tsql_query: {}\n"
                            "\tdb_connection: {}\n"
                            "\tcreate_zip_file: {}\n"
                            "\tmail_to: {}\n"
                            "\tmail_cc: {}\n"
                            "\tmail_bcc: {}\n"
                            "\tmail_subject: {}\n"
                            "\tmail_body: {}\n".format(msg.key(),
                                                      user.report_name,
                                                      user.sql_query,
                                                      user.db_connection,
                                                      user.create_zip_file,
                                                      user.mail_to,
                                                      user.mail_cc,
                                                      user.mail_bcc,
                                                      user.mail_subject,
                                                      user.mail_body))
                
                
                csv_generation_path = generate_report_file(user.report_name, user.sql_query, user.db_connection,user.create_zip_file)
                        
                if csv_generation_path:
                    try:
                        result = send_email(mail_to=user.mail_to,
                                            mail_cc=user.mail_cc,
                                            mail_bcc=user.mail_bcc,
                                            mail_subject=user.mail_subject,
                                            mail_body=user.mail_body,
                                            mail_attachments=csv_generation_path)
                        logger.info(f'Mail sent for report {user.report_name}, updating in metastore')
                        if result == 1:
                            update_sent(report_id, user.report_name)
                            

                    except Exception as e:
                        logger.exception(f'Error processing report {user.report_name}: {e}')
                else:
                    logger.warning(f'Empty result for SQL query for report {user.report_name}, skipping email sending')

    except KeyboardInterrupt:
        consumer.close()

if __name__ == "__main__":
    main()

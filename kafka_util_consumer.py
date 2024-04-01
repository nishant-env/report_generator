import os
import json
from config import schema_registry_url,bootstrap_server
from utils import logger, generate_report_file, send_email, update_sent, get_schema ,User,consumer_conf
from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.serialization import StringDeserializer

decoder = StringDeserializer(codec='utf_8')
offsets_to_commit = set()
def user_to_dict(obj, ctx):
    if obj is None:
        return None

    return User(report_name=obj['report_name'], sql_query=obj['sql_query'], db_connection=obj['db_connection'],
                create_zip_file=obj['create_zip_file'], mail_to=obj['mail_to'], mail_cc=obj['mail_cc'],
                mail_bcc=obj['mail_bcc'], mail_subject=obj['mail_subject'], mail_body=obj['mail_body'])


def main():
    schema = get_schema()
    topic = "esewa3"  
    schema_registry_conf = {'url': schema_registry_url}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    
    avro_deserializer = AvroDeserializer(schema_registry_client, schema, user_to_dict)

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic]) 
    logger.info('Subscribed to the topic')
   

    try:
        while True:
            logger.debug("Polling")
            msg = consumer.poll(timeout=3.0)
            if msg is None:
                print("No messages")
                continue
            
            if msg:
                if not msg.error():
                    logger.info(msg.value())
                    logger.info(msg.key())
                    logger.info(msg.topic())
                    logger.info(msg.partition())
                    logger.info(msg.offset())
                    logger.info(msg)
                    offsets_to_commit.add((msg.topic(),msg.partition(), msg.offset()))

                    # Commit offsets of processed messages
                    if offsets_to_commit:
                        topic_partitions = [TopicPartition(topic, partition, offset) for topic, partition, offset in offsets_to_commit]
                        consumer.commit(offsets=topic_partitions)
                        print("Committed offsets:", offsets_to_commit)
        

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.info('End of partition')
                    continue
                else:
                    raise KafkaException(msg.error())
            report_id = msg.key().decode('utf-8').split('-')[0]
            user = avro_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
    
            if user is not None:
                logger.debug("Received User record {}: report_name: {}\n"
                            "\tsql_query: {}\n"
                            "\tdb_connection: {}\n"
                            "\tcreate_zip_file: {}\n"
                            "\tmail_to: {}\n"
                            "\tmail_cc: {}\n"
                            "\tmail_bcc: {}\n"
                            "\tmail_subject: {}\n"
                            "\tmail_body: {}\n".format(msg.key(),user.report_name,user.sql_query,user.db_connection,user.create_zip_file,user.mail_to,user.mail_cc,user.mail_bcc,user.mail_subject,user.mail_body))
                
                logger.info(f'report {user.report_name}, sending mail now')
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

    except KeyboardInterrupt:
        consumer.close()

if __name__ == "__main__":
    main()

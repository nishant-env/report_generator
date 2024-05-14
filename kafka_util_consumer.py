from utils import logger, generate_report_file, send_email, update_sent ,consumer_conf, avro_deserialization_formatter, update_time_taken, update_last_error
from confluent_kafka import Consumer, KafkaError, KafkaException
from config import kafka_topic
import time
import datetime

def assignmentCallback(_,partitions):
    logger.info('Subscription Successful')
    for partition in partitions:
        logger.info(f'Subscribed to topic {partition.topic}, partition {partition.partition}, offset {partition.offset}')
    
    logger.info('-----------------------------------------------------------------------------')

def revokationCallback(_,partitions):
    logger.info("Revocation Triggered")
    for partition in partitions:
        logger.info(f'Revoked from {partition.topic}, partition {partition.partition}, offset {partition.offset}')
    logger.info('-----------------------------------------------------------------------------')

def main(): 

    consumer = Consumer(consumer_conf)
    consumer.subscribe([kafka_topic], on_assign=assignmentCallback, on_revoke=revokationCallback) 
   

    try:
        while True:
            logger.debug("Polling")
            msg = consumer.poll(timeout=3.0)
            if msg is None:
                logger.debug("No messages")
                continue
            
            if msg:
                if not msg.error():
                    logger.debug(msg.value())
                    logger.debug(msg.key())
                    logger.debug(msg.topic())
                    logger.debug(msg.partition())
                    logger.debug(msg.offset())

                    # Commit offsets of processed messages
        
                    # topic_partition = TopicPartition(msg.topic(), msg.partition(), msg.offset())
                    consumer.commit()
                    logger.info(f"Committed on partition {msg.partition()} offset: {msg.offset()}")
                    # time.sleep(60)
        

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.info('End of partition')
                    continue
                else:
                    raise KafkaException(msg.error())
            report_id = msg.key().decode('utf-8').split('-')[0]
            try:
                report = avro_deserialization_formatter(msg.value())
            except Exception as e:
                logger.exception('Error deserializing the message', e)
                update_last_error(report_id, error_message=f'{str(datetime.datetime.now())} - Error deseralizing the message')

    
            if report is not None:
                logger.debug("Received report record {}: report_name: {}\n"
                            "\tsql_query: {}\n"
                            "\tdb_connection: {}\n"
                            "\tcreate_zip_file: {}\n"
                            "\tmail_to: {}\n"
                            "\tmail_cc: {}\n"
                            "\tmail_bcc: {}\n"
                            "\tmail_subject: {}\n"
                            "\tmail_body: {}\n".format(msg.key(),report.report_name,report.sql_query,report.db_connection,report.create_zip_file,report.mail_to,report.mail_cc,report.mail_bcc,report.mail_subject,report.mail_body))
                
                logger.info(f'Generating report file for: {report.report_name}')
                start_time = time.time()
                csv_generation_path = generate_report_file(report_id, report.report_name, report.sql_query, report.db_connection,report.create_zip_file, report.type, report.logical_date)
                end_time = time.time()
                time_taken = max(1,int(end_time - start_time))
                      
                if csv_generation_path:
                    try:
                        logger.info(f'Report Generated, took {time_taken}s, sending mail now')
                        update_time_taken(report_id, report.report_name, time_taken)
                        result = send_email(mail_to=report.mail_to,
                                            mail_cc=report.mail_cc,
                                            mail_bcc=report.mail_bcc,
                                            mail_subject=report.mail_subject,
                                            mail_body=report.mail_body,
                                            mail_attachments=csv_generation_path)
                        if result == 1:
                            logger.info(f'Mail sent for report {report.report_name}, updating in metastore')
                            update_sent(report_id, report.report_name)
                        else:
                            update_last_error(report_id=report_id, error_message = f'{str(datetime.datetime.now())} - Error sending email')
                            
                    except Exception as e:
                        logger.exception(f'Error sending mail {report.report_name}: {e}')
                else:
                    logger.info('No file generated, skipping')

    except KeyboardInterrupt:
        consumer.close()

if __name__ == "__main__":
    main()

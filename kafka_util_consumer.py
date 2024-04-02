from utils import logger, generate_report_file, send_email, update_sent ,consumer_conf, avro_deserialization_formatter
from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
from config import kafka_topic

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
        
                    topic_partition = TopicPartition(msg.topic(), msg.partition(), msg.offset())
                    consumer.commit(offsets=[topic_partition])
                    logger.info(f"Committed on partition {msg.partition()} offset: {msg.offset()}")
        

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.info('End of partition')
                    continue
                else:
                    raise KafkaException(msg.error())
            report_id = msg.key().decode('utf-8').split('-')[0]
            report = avro_deserialization_formatter(msg.value())
    
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
                csv_generation_path = generate_report_file(report.report_name, report.sql_query, report.db_connection,report.create_zip_file)

                      
                if csv_generation_path:
                    try:
                        result = send_email(mail_to=report.mail_to,
                                            mail_cc=report.mail_cc,
                                            mail_bcc=report.mail_bcc,
                                            mail_subject=report.mail_subject,
                                            mail_body=report.mail_body,
                                            mail_attachments=csv_generation_path)
                        if result == 1:
                            logger.info(f'Mail sent for report {report.report_name}, updating in metastore')
                            update_sent(report_id, report.report_name)
                            
                    except Exception as e:
                        logger.exception(f'Error processing report {report.report_name}: {e}')

    except KeyboardInterrupt:
        consumer.close()

if __name__ == "__main__":
    main()

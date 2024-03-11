import json
from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
from utils import logger, generate_report_file, send_email, update_sent


# consumer basic config
consumer_conf = {
    'bootstrap.servers': 'localhost:9094',
    'group.id' : 'group_1_1',
    'auto.offset.reset': 'smallest'
}


## initializing the consumer
consumer = Consumer(consumer_conf)
consumer.subscribe(['generate-report'])
logger.info('Got the subscription')


# writing main consumer engine
running=True

while running:
    try:
        logger.info("Polling")
        msg = consumer.poll(timeout=3.0)
        if msg is None:
            print("No messages")
            continue

        elif msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print('End of partition')
            else:
                raise KafkaException(msg.error())
        else:
            #### process the individual reports here
            report_id = msg.key().decode('utf-8').split('-')[0]
            report_metadata = json.loads(msg.value().decode('utf-8'))
            csv_generation_path = generate_report_file(report_metadata["report_name"], report_metadata['sql_query'], report_metadata['db_connection'])
            logger.info(f'Generated csv file for report {report_metadata["report_name"]}, sending mail now')
            try:
                result = send_email(mail_to=report_metadata['mail_to'],
                       mail_cc=report_metadata['mail_cc'],
                       mail_bcc=report_metadata['mail_bcc'],
                       mail_subject=report_metadata['mail_subject'],
                       mail_body=report_metadata['mail_body'],
                       mail_attachments=csv_generation_path
                       )
                logger.info(f'Mail sent for report {report_metadata["report_name"]}, updating in metastore')
                if result == 1:
                    update_sent(report_id, report_metadata['report_name'])

            except Exception as e:
                logger.exception(f'Error sending mail for report {report_metadata["report_name"]}, {e}')
    except KeyboardInterrupt:
        consumer.close()            

consumer.close()

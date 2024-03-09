import json
from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
from log_utils import logger
from db_utils import generate_report_file

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
    logger.info("Polling")
    msg = consumer.poll(timeout=3.0, max_poll_records=2)
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
        generate_report_file(report_metadata["report_name"], report_metadata['sql_query'], report_metadata['db_connection'])




consumer.close()

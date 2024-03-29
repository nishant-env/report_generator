from json import loads
from confluent_kafka import Producer
from sqlalchemy import update, func
from .log_utils import logger
from .db_utils import update_last_scheduled
from models import Reports
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer


decoder = StringDeserializer(codec='utf_8')

## defining custom partitioner
count=0
def partitioner(key):
    global count  # global keyword here allows the global variable to be changed by local scope
    ## assuming 3 partitions, for long running query, use a single partition, else a round robin partitioner on remaining 2
    available_partitions = [0,1]
    long_short = decoder(key).split('-')[-1]
    if long_short.lower() == 'l':
        return 2
    else:
        return_partition = available_partitions[count]
        count=count+1
        if count > 1:
            count=0
        return return_partition




conf = {'bootstrap.servers': "localhost:19092", 'client.id': 'producer_1'}
producer = Producer(conf)


def produced_callback(error, message):
    if error is not None:
        logger.exception(f'Error Occured for report {error.key()}')
    else:
        logger.info(f'Sucessfully sent to queue for {message.key()}')
        ### setting the last scheduled time here 
        update_last_scheduled(
            report_id=message.key().decode('utf-8').split('-')[0],
        )
        


def send_report_to_queue(producer, key, value):
        producer.produce(
            topic='report3',
            key=key,
            value=value,
            partition=partitioner(key),
            callback=produced_callback
        )
        producer.poll(2)


def flush_producer(producer):
    producer.flush()

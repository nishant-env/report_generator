from json import loads
from confluent_kafka import Producer
from .log_utils import logger
from .configuration_utils import producer_conf
from .db_utils import update_last_scheduled
from .avro_utils import avro_serialization_formatter, avro_deserialization_formatter
from confluent_kafka.serialization import StringSerializer, StringDeserializer
from config import kafka_topic



producer = Producer(producer_conf)
encoder = StringSerializer(codec='utf_8')
decoder = StringDeserializer(codec='utf_8')

## defining custom partitioner
count=0
def partitioner(key):
    global count 
    ## assuming 3 partitions, for long running query, use a single partition, else a round robin partitioner on remaining 2
    available_partitions = [0, 1]
    long_short = decoder(key).split('-')[-1]
    if long_short.lower() == 'l':
        return 2
    else:
        return_partition = available_partitions[count]
        count=count+1
        if count > 1:
            count=0
        return return_partition





def produced_callback(error, message):
    if error is not None:
        logger.exception(f'Error Occured for report {error.key()}')
    else:
        logger.info(f'Report_id: {message.key()}, report_name: {avro_deserialization_formatter(message.value()).report_name} successfully sent to queue on topic {message.topic()} at partition {message.partition()} at offset {message.offset()}')
        ### setting the last scheduled time here 
        update_last_scheduled(
            report_id=message.key().decode('utf-8').split('-')[0])
        


def send_report_to_queue(key, value):
        key = encoder(key)
        serialized_value = avro_serialization_formatter(value)
        if serialized_value:
            producer.produce(
                topic=kafka_topic,
                key=key,
                value=serialized_value,
                partition=partitioner(key),
                callback=produced_callback
            )
        else:
            logger.info("Cannot send this message to queue")
        producer.poll(2)



def flush_producer():
    producer.flush()

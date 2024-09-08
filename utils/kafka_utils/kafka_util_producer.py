from json import loads
from confluent_kafka import Producer
from ..log_utils import logger
from ..db_utils import update_last_scheduled, update_last_error
from confluent_kafka.serialization import StringSerializer, StringDeserializer
from config import kafka_topic, kafka_topic_num_partitions
import datetime

class ProducerUtility:
    def __init__(self, producer_conf: dict, serializer, deserializer):
        self.producer_instance = Producer(producer_conf)
        self.encoder = StringSerializer(codec='utf_8')
        self.decoder = StringDeserializer(codec='utf_8')
        self.serializer = serializer
        self.deserializer = deserializer
        self.counter_for_partition = 0

    def __getPartitionByKey(self, partitioner_flag: str) -> int:
        """
        We'll allocate a single partition for long running queries, while all other partitions will be used in round robin.
        This is chosen so that long running query won't interrupt other reports
        """
        available_partitions_for_rr = [x for x in range(kafka_topic_num_partitions - 1)]
        if partitioner_flag.lower == 'l':
            return kafka_topic_num_partitions - 1    # last partition
        else:
            return_partition = available_partitions_for_rr[self.counter_for_partition]
            self.counter_for_partition = self.counter_for_partition + 1
            if self.counter_for_partition >= kafka_topic_num_partitions - 1:
                self.counter_for_partition = 0
            return return_partition

    def __produced_callback(self, error, message) -> None:
        try:
            if error is not None:
                logger.exception(f'Error Occured for report {error.key()}')
            else:
                report_id = self.decoder(message.key()).split('-')[0]
                logger.info(f'Report_id: {report_id}, report_name: {self.deserializer(message.value()).report_name} successfully sent to queue on topic {message.topic()} at partition {message.partition()} at offset {message.offset()} with key {self.decoder(message.key())}')
                # setting the last scheduled time here 
                update_last_scheduled(
                    report_id=report_id
                    )
        except Exception as e:
            logger.exception('Error in producer callback')
            logger.exception(e)   

    def send_report_to_queue(self, key: str, value: dict, partitioner_flag: str = 's') -> None:
        try:
            encoded_key = self.encoder(key)
            serialized_value = self.serializer(value)
            if serialized_value:
                self.producer_instance.produce(
                    topic=kafka_topic,
                    key=encoded_key,
                    value=serialized_value,
                    partition=self.__getPartitionByKey(partitioner_flag),
                    callback=self.__produced_callback
                )
            else:
                logger.info("Cannot send this message to queue")
                update_last_error(report_id=key.split('-')[0], error_message=f'{datetime.datetime.now()} - Error Serializing message')
            self.producer_instance.poll(2)
        except Exception as e:
            logger.error('Error sending report to the queue')
            logger.exception(e)

    def flush_producer(self):
        self.producer_instance.flush()

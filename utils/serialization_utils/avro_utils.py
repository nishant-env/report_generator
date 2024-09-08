from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
from ..log_utils import logger
from models import ReportModel


class AvroUtil():
    def __init__(self, schema_registry_url: str, avro_schema_str: str, kafka_topic: str, serialization_model: object):
        self.schema_registry_client = SchemaRegistryClient({'url': schema_registry_url})
        self.avro_schema_str = avro_schema_str
        self.kafka_topic = kafka_topic
        self.serialization_model = serialization_model

    ## Serializer definition
    def avro_serialization_formatter(self, dict_for_serialization: dict) -> bytes:
        try:
            object_for_serialization = self.serialization_model(**dict_for_serialization)
            avro_serializer = AvroSerializer(self.schema_registry_client, self.avro_schema_str, object_for_serialization.get_dictionary_formatted)
            serialized_object = avro_serializer(object_for_serialization, SerializationContext(self.kafka_topic, MessageField.VALUE))
            return serialized_object
        except Exception as e:
            logger.exception(f'Error Serializing Data {e}')
            return None
    
    def avro_deserialization_formatter(self, object_for_deserialization: bytes):
        try:
            if object_for_deserialization is None:
                return None
            avro_deserializer = AvroDeserializer(self.schema_registry_client, self.avro_schema_str, self.serialization_model.get_model_from_dict)
            deserialized_object = avro_deserializer(object_for_deserialization, SerializationContext(self.kafka_topic, MessageField.VALUE))
            return deserialized_object
        except Exception as e:
            logger.exception(f'Error Deserializing Data {e}')

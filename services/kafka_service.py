import json
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

class KafkaService:
    def create_consumer_with_topic(self, topics: list, server: str, group_id: str, auto_offset_reset = 'latest') -> KafkaConsumer:
        """
        Create Kafka consumer object.

        :param topics: List of Topics to subscribe
        :param server: Server Host name

        :return: Kafka Consumer Object
        """
        assert server != '', 'bootstrap_servers cannot be empty'
        assert len(topics) != 0, 'Topics list cannot be empty'
        assert group_id != '', 'group_id cannot be empty'
        assert auto_offset_reset != '', 'auto_offset_reset cannot be empty'

        consumer = KafkaConsumer(bootstrap_servers=server, group_id=group_id, auto_offset_reset=auto_offset_reset)
        consumer.subscribe(topics)
        return consumer

    def produce_stream(self, server: str, topic:str, data: str) -> bool:
        """
        Push data to Kafka Topic.

        :param server: Server Host name
        :param topic: Topic name to push data
        :param data: String data to push to Kafka

        :return: Bool (Success/Failure)
        """
        assert server != '', 'bootstrap_servers cannot be empty'
        assert topic != '', 'Topic cannot be empty'
        assert data != '', 'Data to produce cannot be empty'

        producer = KafkaProducer(bootstrap_servers=server)
        future_result = producer.send(topic, str.encode(data))
        try:
            record_metadata = future_result.get(timeout=10)
            if record_metadata.partition is not None and record_metadata.offset is not None:
                return True
            return False
        except KafkaError as e:
            return False
            pass

    def consume_stream(self, consumer: KafkaConsumer):
        """
        Consume stream and convert bytes to json.

        :param consumer: Kafka Consumer Object
        :return: Yield each json
        """
        for stream in consumer:
            stream_json_object = json.loads(stream.value.decode('utf-8'))
            yield stream_json_object





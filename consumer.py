from kafka import KafkaConsumer,consumer
import json

class MessageConsumer:
    broker = ""
    topic = ""
    group_id = ""
    logger = None

    def __init__(self, broker, topic, group_id):
        self.broker = broker
        self.topic = topic
        self.group_id = group_id

    def consumer_message(self):
        consumer = KafkaConsumer(
            bootstrap_servers = self.broker,
            group_id = 'my_group',
            consumer_timeout_ms = 60000,
            auto_offset_reset = 'earliest',
            enable_auto_commit = False,
            value_deserializer=lambda m: json.loads(m.decode('ascii')) # Decode message values as UTF-8 strings
        )

        consumer.subscribe(self.topic)
        print('consumer is listening..')
        try:
            for message in consumer:
                print("Received message:", message)
                print()
                consumer.commit()
        except Exception as ex:
            print(ex)
        finally:
            consumer.close()

broker = 'localhost:9092'
topic = 'testTopic'
group_id = 'group1'

consumer1 = MessageConsumer(broker, topic, group_id)
consumer1.consumer_message()

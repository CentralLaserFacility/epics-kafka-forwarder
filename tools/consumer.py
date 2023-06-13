from confluent_kafka import Consumer, KafkaException, TopicPartition
from streaming_data_types.epics_connection_info_ep00 import deserialise_ep00
from streaming_data_types.logdata_f142 import deserialise_f142
from streaming_data_types.exceptions import WrongSchemaException
from streaming_data_types.forwarder_config_update_rf5k import deserialise_rf5k
from streaming_data_types.status_x5f2 import deserialise_x5f2
import datetime as dt
import argparse


parser = argparse.ArgumentParser(description="Reads the Messages in Kafka topic")

parser.add_argument(
    "--broker",
    "-b",
    default="localhost:9092",
    help="name:port of Kafka brokers",
    action="append",
)
parser.add_argument(
    "--group",
    "-g",
    default="consumer_group_name",
    help="Enter the consumer group name",
)
parser.add_argument(
    "--topic",
    "-t",
    default="output-topic",
    help="Kafka topics to be read.",
    required=True,
)
parser.add_argument(
    "--offset",
    choices=("latest", "earliest", "none"),
    default="latest",
    help="Enter if messages should be from earliest or only latest",
)

args = parser.parse_args()


broker = args.broker
group = args.group
conf = {
    "bootstrap.servers": broker,
    "group.id": group,
    "auto.offset.reset": args.offset,
    "enable.auto.commit": False,
}
consumer = Consumer(conf)
topic = args.topic

# consumer.subscribe([topic])

metadata = consumer.list_topics(topic)
# 1684240347940
timestamp_ms = 1605272440696
topic_partitions = [
    TopicPartition(topic, partition[1].id, offset=timestamp_ms)
    for partition in metadata.topics[topic].partitions.items()
]
topic_partitions = consumer.offsets_for_times(topic_partitions)
consumer.assign(topic_partitions)

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())
        else:
            if msg.offset() >= 0:
                print(
                    f"{msg.topic()} [{msg.partition()}] at offset {msg.offset()} with key {str(msg.key())}:"
                )
                schema = msg.value()[4:8]

                # print(schema)

                if schema == b"x5f2":
                    res = deserialise_x5f2(msg.value())
                    print(res)
                elif schema == b"ep00":
                    print(deserialise_ep00(msg.value()))
                elif schema == b"f142":
                    res = deserialise_f142(msg.value())
                    print(
                        f"PV Name : {res.source_name},Timestamp : {res.timestamp_unix_ns}"
                    )
                    print(f"Data : {res.value} \n\n")
                elif schema == b"rf5k":
                    res = deserialise_rf5k(msg.value())
                    print(f"{res.streams}\n")

except KeyboardInterrupt:
    pass

finally:
    consumer.close()

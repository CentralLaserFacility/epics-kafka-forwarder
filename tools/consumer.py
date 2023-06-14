from confluent_kafka import Consumer, KafkaException, TopicPartition
from streaming_data_types.epics_connection_info_ep00 import deserialise_ep00
from streaming_data_types.logdata_f142 import deserialise_f142
from streaming_data_types.exceptions import WrongSchemaException
from streaming_data_types.forwarder_config_update_rf5k import deserialise_rf5k
from streaming_data_types.fbschemas.forwarder_config_update_rf5k.UpdateType import UpdateType
from streaming_data_types.status_x5f2 import deserialise_x5f2
import datetime as dt
import argparse
import secrets


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
    help="The consumer group name. If not specified, a random one will be chosen.",
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
    choices=("latest", "earliest"),
    default="latest",
    help="Enter if messages should be from earliest or only latest",
)
parser.add_argument(
    "--verbose",
    "-v",
    action="store_true",
    help="display additional information",
)

args = parser.parse_args()


broker = args.broker
group = args.group
if group is None:
    # Choose a random group name. We want the convenience of subscribe, but not the grouping behaviour.
    group = f"epics-kafka-forwarder/consumer@{secrets.token_hex(8)}"
conf = {
    "bootstrap.servers": broker,
    "group.id": group,
    "auto.offset.reset": args.offset,
    "enable.auto.commit": False,
}
consumer = Consumer(conf)
topic = args.topic

# This seems to make everything a bit slow on startup. We might want to switch to assign()
consumer.subscribe([topic])

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())
        else:
            if len(msg.value()) > 8:
                if args.verbose:
                    print(
                        f"{msg.topic()} [{msg.partition()}] at offset {msg.offset()}:"
                    )
                schema = msg.value()[4:8]

                if schema == b"x5f2":
                    res = deserialise_x5f2(msg.value())
                    print(res)
                elif schema == b"ep00":
                    print(deserialise_ep00(msg.value()))
                elif schema == b"f142":
                    res = deserialise_f142(msg.value())
                    timestamp = dt.datetime.fromtimestamp(res.timestamp_unix_ns / 1e9)
                    print(
                        f"{res.source_name}  {timestamp.isoformat()}  {res.value}"
                    )
                elif schema == b"rf5k":
                    res = deserialise_rf5k(msg.value())
                    if res.config_change == UpdateType.ADD:
                        print(f"config: ADD {res.streams}")
                    elif res.config_change == UpdateType.REMOVE:
                        print(f"config: REMOVE {res.streams}")
                    elif res.config_change == UpdateType.REMOVEALL:
                        print(f"config: REMOVEALL")

except KeyboardInterrupt:
    pass

finally:
    consumer.close()

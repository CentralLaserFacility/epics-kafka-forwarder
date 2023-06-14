from confluent_kafka import Producer
from streaming_data_types.forwarder_config_update_rf5k import (
    serialise_rf5k,
    StreamInfo,
    Protocol,
)
from streaming_data_types.fbschemas.forwarder_config_update_rf5k.UpdateType import (
    UpdateType,
)

import argparse
import os
import sys


def error(msg):
    sys.stderr.write(msg + "\n")
    sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Epics forwarder using kafka",
    )

    parser.add_argument(
        "--broker",
        "-b",
        help="Name:Port of Kafka brokers",
        action="append",
    )
    parser.add_argument(
        "--config-topic",
        "-c",
        default="config-topic",
        help="Configuration kafka topic",
        action="store",
    )
    parser.add_argument(
        "--pv-name",
        "-p",
        help="PV names that needs to be forwarded",
        action="append",
    )
    parser.add_argument(
        "--output-topic",
        "-o",
        help="Kafka topics assoicated with the PVs. Please enter topics and output kafka topic in same order.",
        action="append",
    )

    parser.add_argument(
        "my_args",
        metavar="PV_NAME TOPIC_NAME",
        help="alternating pv names and topic names",
        nargs="*",
    )

    group = parser.add_mutually_exclusive_group(required=True)

    group.add_argument("--add", "-a", action="store_true")
    group.add_argument("--remove", "-r", action="store_true")
    group.add_argument("--removeall", "-R", action="store_true")

    args = parser.parse_args()
    if args.broker:
        CONFIG_BROKER = ",".join(args.broker)
    elif os.environ.get("EPICS_FORWARDER_BROKER"):
        CONFIG_BROKER = os.environ["EPICS_FORWARDER_BROKER"]
    else:
        CONFIG_BROKER = "localhost:9092"

    CONFIG_TOPIC = args.config_topic
    pv_protocol = Protocol.Protocol.CA

    producer = Producer({"bootstrap.servers": CONFIG_BROKER})
    STREAMS = []

    try:
        if args.removeall:
            if args.my_args or args.pv_name or args.output_topic:
                error(
                    "Error: Entered Argumens with RemoveAll. Do you mean -r/--remove?"
                )
            producer.produce(CONFIG_TOPIC, serialise_rf5k(UpdateType.REMOVEALL, []))
        else:
            if not args.my_args:
                if not args.pv_name:
                    error("Error: PV names not specified")
                if not args.output_topic:
                    error("Error: Output topics not specified")

                if len(args.pv_name) == len(args.output_topic):
                    for i in range(len(args.pv_name)):
                        STREAMS.append(
                            StreamInfo(
                                args.pv_name[i],
                                "f142",
                                args.output_topic[i],
                                pv_protocol,
                            )
                        )

            elif len(args.my_args) % 2 != 0:
                error("Error: Equal number of pv names and output topics not entered.")
            else:
                pv_names = args.my_args[::2]
                topics = args.my_args[1::2]

                for i in range(len(pv_names)):
                    # Syntax followed for creating a Stream
                    # StreamInfo("IOC:PV1", "f142", "output topic", Protocol.Protocol.CA)
                    STREAMS.append(
                        StreamInfo(pv_names[i], "f142", topics[i], pv_protocol)
                    )

            if args.add:
                producer.produce(CONFIG_TOPIC, serialise_rf5k(UpdateType.ADD, STREAMS))
            elif args.remove:
                producer.produce(
                    CONFIG_TOPIC, serialise_rf5k(UpdateType.REMOVE, [STREAMS])
                )
        producer.flush()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()

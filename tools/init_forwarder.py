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
    parser = argparse.ArgumentParser(description="Configure the EPICS-Kafka forwarder.")

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
        "my_args",
        metavar="PV_NAME TOPIC_NAME",
        help="alternating pv names and topic names",
        nargs="*",
    )

    group = parser.add_mutually_exclusive_group(required=True)

    group.add_argument(
        "--add",
        "-a",
        action="store_true",
        help="Add the provided PVs and topics to the forwarder",
    )
    group.add_argument(
        "--remove",
        "-r",
        action="store_true",
        help="Remove the provided PV name/topic pairs from the forwarder",
    )
    group.add_argument(
        "--remove-all",
        "-R",
        action="store_true",
        help="Remove all PVs from the forwarder. Do not specify any PVs or topics.",
    )

    args = parser.parse_args()
    if args.broker:
        config_broker = ",".join(args.broker)
    elif os.environ.get("EPICS_FORWARDER_BROKER"):
        config_broker = os.environ["EPICS_FORWARDER_BROKER"]
    else:
        config_broker = "localhost:9092"

    config_topic = args.config_topic
    pv_protocol = Protocol.Protocol.CA

    producer = Producer({"bootstrap.servers": config_broker})
    streams = []

    try:
        if args.remove_all:
            if args.my_args:
                error(
                    "Error: Entered arguments with --remove-all. Did you mean --remove?"
                )
            producer.produce(config_topic, serialise_rf5k(UpdateType.REMOVEALL, []))
        else:
            if not args.my_args:
                if args.remove:
                    error(
                        "Error: No PV names or topic specified. Did you mean --remove-all?"
                    )
                else:
                    error("Error: No PV names or topics specified.")

            elif len(args.my_args) % 2 != 0:
                error("Error: Equal number of pv names and output topics not entered.")
            else:
                pv_names = args.my_args[::2]
                topics = args.my_args[1::2]

                for i in range(len(pv_names)):
                    # Syntax followed for creating a Stream
                    # StreamInfo("IOC:PV1", "f142", "output topic", Protocol.Protocol.CA)
                    streams.append(
                        StreamInfo(pv_names[i], "f142", topics[i], pv_protocol)
                    )

            if args.add:
                producer.produce(config_topic, serialise_rf5k(UpdateType.ADD, streams))
            elif args.remove:
                producer.produce(
                    config_topic, serialise_rf5k(UpdateType.REMOVE, streams)
                )
        producer.flush()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()

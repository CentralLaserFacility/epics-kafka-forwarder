from caproto.threading.client import Context as CaContext
from p4p.client.thread import Context as PvaContext
from typing import Dict

from forwarder.kafka.kafka_helpers import (
    create_producer,
    create_consumer,
    get_broker_and_topic_from_uri,
)
from forwarder.application_logger import setup_logger
from forwarder.parse_config_update import parse_config_update
from forwarder.status_reporter import StatusReporter
from forwarder.parse_commandline_args import parse_args, get_version
from forwarder.handle_config_change import handle_configuration_change


if __name__ == "__main__":
    args = parse_args()

    logger = setup_logger(
        level=args.verbosity,
        log_file_name=args.log_file,
        graylog_logger_address=args.graylog_logger_address,
    )
    version = get_version()
    logger.info(f"Forwarder v{version} started, service Id: {args.service_id}")

    # EPICS
    ca_ctx = CaContext()
    pva_ctx = PvaContext("pva", nt=False)
    update_handlers: Dict = dict()

    # Kafka
    producer = create_producer(args.output_broker)
    config_broker, config_topic = get_broker_and_topic_from_uri(args.config_topic)
    consumer = create_consumer(config_broker)
    consumer.subscribe([config_topic])

    status_broker, status_topic = get_broker_and_topic_from_uri(args.status_topic)
    status_reporter = StatusReporter(
        update_handlers,
        create_producer(status_broker),
        status_topic,
        args.service_id,
        version,
        logger,
    )
    status_reporter.start()

    # Metrics
    # use https://github.com/Jetsetter/graphyte ?
    # https://julien.danjou.info/atomic-lock-free-counters-in-python/

    try:
        while True:
            msg = consumer.poll(timeout=0.5)
            if msg is None:
                continue
            if msg.error():
                logger.error(msg.error())
            else:
                logger.info("Received config message")
                config_change = parse_config_update(msg.value())
                handle_configuration_change(
                    config_change,
                    args.fake_pv_period,
                    args.pv_update_period,
                    update_handlers,
                    producer,
                    ca_ctx,
                    pva_ctx,
                    logger,
                    status_reporter,
                )

    except KeyboardInterrupt:
        logger.info("%% Aborted by user")

    finally:
        status_reporter.stop()
        for _, handler in update_handlers.items():
            handler.stop()
        consumer.close()
        producer.close()

from forwarder.update_handlers.create_update_handler import create_update_handler
from forwarder.parse_config_update import Channel, EpicsProtocol
from tests.kafka.fake_producer import FakeProducer
from tests.test_helpers.p4p_fakes import FakeContext
from forwarder.update_handlers.pva_update_handler import PVAUpdateHandler
import logging
import pytest


_logger = logging.getLogger("stub_for_use_in_tests")
_logger.addHandler(logging.NullHandler())


def test_create_update_handler_throws_if_channel_has_no_name():
    producer = FakeProducer()
    channel_with_no_name = Channel(None, EpicsProtocol.PVA, "output_topic", "f142")
    with pytest.raises(RuntimeError):
        create_update_handler(producer, None, None, channel_with_no_name, 20000)  # type: ignore

    channel_with_empty_name = Channel("", EpicsProtocol.PVA, "output_topic", "f142")
    with pytest.raises(RuntimeError):
        create_update_handler(producer, None, None, channel_with_empty_name, 20000)  # type: ignore


def test_create_update_handler_throws_if_channel_has_no_topic():
    producer = FakeProducer()
    channel_with_no_topic = Channel("name", EpicsProtocol.PVA, None, "f142")
    with pytest.raises(RuntimeError):
        create_update_handler(producer, None, None, channel_with_no_topic, 20000)  # type: ignore

    channel_with_empty_topic = Channel("name", EpicsProtocol.PVA, "", "f142")
    with pytest.raises(RuntimeError):
        create_update_handler(producer, None, None, channel_with_empty_topic, 20000)  # type: ignore


def test_create_update_handler_throws_if_channel_has_no_schema():
    producer = FakeProducer()
    channel_with_no_topic = Channel("name", EpicsProtocol.PVA, "output_topic", None)
    with pytest.raises(RuntimeError):
        create_update_handler(producer, None, None, channel_with_no_topic, 20000)  # type: ignore

    channel_with_empty_topic = Channel("name", EpicsProtocol.PVA, "output_topic", "")
    with pytest.raises(RuntimeError):
        create_update_handler(producer, None, None, channel_with_empty_topic, 20000)  # type: ignore


def test_create_update_handler_throws_if_protocol_not_specified():
    producer = FakeProducer()
    channel_with_no_protocol = Channel(
        "name", EpicsProtocol.NONE, "output_topic", "f142"
    )
    with pytest.raises(RuntimeError):
        create_update_handler(producer, None, None, channel_with_no_protocol, 20000)  # type: ignore


def test_pva_handler_created_when_pva_protocol_specified():
    producer = FakeProducer()
    context = FakeContext()
    channel_with_pva_protocol = Channel(
        "name", EpicsProtocol.PVA, "output_topic", "f142"
    )
    handler = create_update_handler(producer, None, context, channel_with_pva_protocol, 20000)  # type: ignore
    assert isinstance(handler, PVAUpdateHandler)

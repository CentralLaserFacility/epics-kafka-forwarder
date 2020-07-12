from streaming_data_types.forwarder_config_update_rf5k import serialise_rf5k
from streaming_data_types.fbschemas.forwarder_config_update_rf5k.UpdateType import (
    UpdateType,
)
from forwarder.parse_config_update import parse_config_update, CommandType


def test_parses_removeall_config_type():
    message = serialise_rf5k(UpdateType.REMOVEALL, [])
    config_update = parse_config_update(message)
    assert config_update.command_type == CommandType.REMOVE_ALL


def test_parses_remove_config_type():
    message = serialise_rf5k(UpdateType.REMOVE, [])
    config_update = parse_config_update(message)
    assert config_update.command_type == CommandType.REMOVE_ALL


def test_parses_add_config_type():
    message = serialise_rf5k(UpdateType.ADD, [])
    config_update = parse_config_update(message)
    assert config_update.command_type == CommandType.REMOVE_ALL

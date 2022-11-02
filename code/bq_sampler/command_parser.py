# vim: ai:sw=4:ts=4:sta:et:fo=croql
"""
Parses a dictionary into an instance of :py:class:`command: command.CommandBase`.
"""
from typing import Any, Dict, Optional

from bq_sampler.entity import command
from bq_sampler import const, logger

_LOGGER = logger.get(__name__)


def to_command(value: Dict[str, Any], timestamp: int) -> command.CommandBase:
    """
    Converts a dictionary to the corresponding
        :py:class:`command.CommandBase` subclass.

    :param value:
    :param timestamp:
    :return:
    """
    _LOGGER.debug(
        'Converting value into a %s instance with timestamp %s. Value: %s',
        command.CommandBase.__name__,
        timestamp,
        value,
    )
    # validate input
    req_type = _validate_command_dict_and_get_request_type(value)
    if not req_type:
        # see if it is a transfer notification
        value_new = _from_transfer_run_notification(value)
        req_type = _validate_command_dict_and_get_request_type(value_new)
        if req_type:
            value = value_new
    if not isinstance(timestamp, int) or timestamp <= 0:
        raise ValueError(
            f'Timestamp must be a positive {int.__name__}. Got <{timestamp}>({type(timestamp)})'
        )
    # logic
    value[command.CommandBase.timestamp.__name__] = timestamp
    if req_type == command.CommandType.START:
        result = command.CommandStart.from_dict(value)
    elif req_type == command.CommandType.SAMPLE_POLICY_PREFIX:
        result = command.CommandSamplePolicyPrefix.from_dict(value)
    elif req_type == command.CommandType.SAMPLE_START:
        result = command.CommandSampleStart.from_dict(value)
    elif req_type == command.CommandType.SAMPLE_DONE:
        result = command.CommandSampleDone.from_dict(value)
    elif req_type == command.CommandType.TRANSFER_RUN_DONE:
        result = command.CommandTransferRunDone.from_dict(value)
    elif req_type == command.CommandType.REMOVE_DATASET:
        result = command.CommandRemoveDataset.from_dict(value)
    else:
        raise ValueError(f'Command type <{req_type}> is not supported. Argument: <{value}>')
    return result


def _validate_command_dict_and_get_request_type(value: Dict[str, Any]) -> command.CommandType:
    if not isinstance(value, dict):
        raise TypeError(f'Expecting a {dict.__name__} as argument. Got: <{value}>({type(value)})')
    return command.CommandType.from_str(value.get(command.CommandBase.type.__name__))


def _from_transfer_run_notification(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    # pylint: disable=line-too-long
    """
    Example payload::
        {
          "dataSourceId": "cross_region_copy",
          "destinationDatasetId": "new_york_taxi_trips",
          "emailPreferences": {

          },
          "endTime": "2022-10-26T13:36:24.042815Z",
          "errorStatus": {

          },
          "name": "projects/831514123984/locations/europe-west4/transferConfigs/63592c65-0000-2ee6-9089-30fd3811ab04/runs/635917b1-0000-2fdd-9bf9-089e08257ad8",
          "notificationPubsubTopic": "projects/tgt-bq/topics/test-trnsfer",
          "params": {
            "overwrite_destination_table": true,
            "source_dataset_id": "new_york_taxi_trips_europe_west3_temp",
            "source_project_id": "tgt-bq"
          },
          "runTime": "2022-10-26T13:35:08.333Z",
          "scheduleTime": "2022-10-26T13:35:08.810982Z",
          "state": "SUCCEEDED",
          "updateTime": "2022-10-26T13:36:24.042825Z",
          "userId": "4735888344461745670"
        }
    :return:
    """
    # pylint: enable=line-too-long
    _LOGGER.debug(
        'Converting payload into %s command. Payload: %s',
        command.CommandType.TRANSFER_RUN_DONE.value,
        payload,
    )
    result = None
    run_name = payload.get(const.TRANSFER_RUN_NAME_ATTR)
    data_source_id = payload.get('dataSourceId')
    params = payload.get('params')
    if isinstance(run_name, str) and isinstance(data_source_id, str) and isinstance(params, dict):
        name_match = const.TRANSFER_CONFIG_FROM_RUN_NAME_RE.match(run_name)
        if name_match:
            # pylint: disable=line-too-long
            result = {
                command.CommandTransferRunDone.type.__name__: command.CommandType.TRANSFER_RUN_DONE.value,
                command.CommandTransferRunDone.name.__name__: name_match.group(1),
                command.CommandTransferRunDone.payload.__name__: payload,
            }
            # pylint: enable=line-too-long
    else:
        _LOGGER.debug(
            'Could not convert payload into %s command. Payload: %s',
            command.CommandType.TRANSFER_RUN_DONE.value,
            payload,
        )
    return result

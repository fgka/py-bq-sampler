# vim: ai:sw=4:ts=4:sta:et:fo=croql
"""
Processes a request coming from Cloud Function.
"""
import os
import time

import cachetools

from bq_sampler.entity import command, table, policy
from bq_sampler import sampler_bucket, sampler_query
from bq_sampler.gcp import pubsub
from bq_sampler import logger

_LOGGER = logger.get(__name__)

_BQ_TARGET_PROJECT_ID_ENV_VAR: str = 'TARGET_PROJECT_ID'
_GCS_POLICY_BUCKET_ENV_VAR: str = 'POLICY_BUCKET_NAME'
_GCS_DEFAULT_POLICY_OBJECT_PATH_ENV_VAR: str = 'DEFAULT_POLICY_OBJECT_PATH'
_GCS_REQUEST_BUCKET_ENV_VAR: str = 'REQUEST_BUCKET_NAME'
_PUBSUB_CMD_TOPIC_ENV_VAR: str = 'CMD_TOPIC_NAME'
_PUBSUB_ERROR_TOPIC_ENV_VAR: str = 'ERROR_TOPIC_NAME'

_PUBSUB_ERROR_CMD_ENTRY: str = 'command'
_PUBSUB_ERROR_MSG_ENTRY: str = 'error'


class _GeneralConfig:
    def __init__(self):
        self._target_project_id = os.environ.get(_BQ_TARGET_PROJECT_ID_ENV_VAR)
        self._policy_bucket = os.environ.get(_GCS_POLICY_BUCKET_ENV_VAR)
        self._default_policy_path = os.environ.get(_GCS_DEFAULT_POLICY_OBJECT_PATH_ENV_VAR)
        self._request_bucket = os.environ.get(_GCS_REQUEST_BUCKET_ENV_VAR)
        self._pubsub_request = os.environ.get(_PUBSUB_CMD_TOPIC_ENV_VAR)
        self._pubsub_error = os.environ.get(_PUBSUB_ERROR_TOPIC_ENV_VAR)

    @property
    def target_project_id(self) -> str:  # pylint: disable=missing-function-docstring
        return self._target_project_id

    @property
    def policy_bucket(self) -> str:  # pylint: disable=missing-function-docstring
        return self._policy_bucket

    @property
    def default_policy_path(self) -> str:  # pylint: disable=missing-function-docstring
        return self._default_policy_path

    @property
    def request_bucket(self) -> str:  # pylint: disable=missing-function-docstring
        return self._request_bucket

    @property
    def pubsub_request(self) -> str:  # pylint: disable=missing-function-docstring
        return self._pubsub_request

    @property
    def pubsub_error(self) -> str:  # pylint: disable=missing-function-docstring
        return self._pubsub_error


def process(cmd: command.CommandBase) -> None:
    """
    Single entry point to process commands coming from Pub/Sub.

    :param cmd:
    :return:
    """
    _LOGGER.debug('Processing command <%s>', cmd)
    try:
        _process(cmd)
        _LOGGER.debug('Processed command <%s>', cmd)
    except Exception as err:
        error_data = {
            _PUBSUB_ERROR_CMD_ENTRY: cmd.as_dict(),
            _PUBSUB_ERROR_MSG_ENTRY: str(err),
        }
        pubsub.publish(error_data, _general_config().pubsub_error)
        msg = f'Could not process command: <{cmd}>. Error: {err}'
        _LOGGER.critical(msg)
        raise RuntimeError(msg) from err


def _process(cmd: command.CommandBase) -> None:
    if cmd.type == command.CommandType.START.value:
        _process_start(cmd)
    elif cmd.type == command.CommandType.SAMPLE_START.value:
        _process_sample_start(cmd)
    elif cmd.type == command.CommandType.SAMPLE_DONE.value:
        _process_sample_done(cmd)
    else:
        raise ValueError(f'Command type <{cmd.type}> cannot be processed')


def _process_start(cmd: command.CommandStart) -> None:
    """
    Will inspect the policy and requests buckets,
        apply compliance,
        and issue a sampling request for all targeted tables.
    It will also generate a :py:class:`command.CommandSampleStart` for each one
        and send it out into the Pub/Sub topic.

    :param cmd:
    :return:
    """
    _LOGGER.debug('Retrieving all policies from bucket <%s>', _general_config().policy_bucket)
    for table_policy in sampler_bucket.all_policies(
        bucket_name=_general_config().policy_bucket,
        default_policy_object_path=_general_config().default_policy_path,
    ):
        _LOGGER.info(
            'Retrieving request, if existent, for table <%s> from bucket <%s>',
            table_policy,
            _general_config().request_bucket,
        )
        table_sample = sampler_bucket.sample_request_from_policy(
            bucket_name=_general_config().request_bucket,
            table_policy=table_policy,
        )
        # compliance enforcement
        table_sample = _compliant_sample_request(table_policy, table_sample)
        # create sample request event
        start_sample_req = _create_sample_start_cmd(cmd, table_policy, table_sample)
        # send request out
        _publish_cmd_to_pubsub(start_sample_req)


@cachetools.cached(cache=cachetools.LRUCache(maxsize=1))
def _general_config() -> _GeneralConfig:
    return _GeneralConfig()


def _compliant_sample_request(
    table_policy: policy.TablePolicy, table_sample: table.TableSample
) -> table.TableSample:
    row_count = sampler_query.row_count(table_sample.table_reference)
    return table_policy.compliant_sample(table_sample, row_count)


def _create_sample_start_cmd(
    cmd: command.CommandStart,
    table_policy: policy.TablePolicy,
    table_sample: table.TableSample,
) -> command.CommandSampleStart:
    source_table = table_policy.table_reference
    kwargs = {
        command.CommandSampleStart.type.__name__: command.CommandType.SAMPLE_START.value,
        command.CommandSampleStart.timestamp.__name__: cmd.timestamp,
        command.CommandSampleStart.sample_request.__name__: table_sample,
        command.CommandSampleStart.target_table.__name__: source_table.clone(
            project_id=_general_config().target_project_id
        ),
    }
    return command.CommandSampleStart(**kwargs)


def _publish_cmd_to_pubsub(cmd: command.CommandBase) -> None:
    topic = _general_config().pubsub_request
    _LOGGER.debug('Sending event request <%s> to topic <%s>', cmd, topic)
    data = cmd.as_dict()
    pubsub.publish(data, topic)


def _process_sample_start(cmd: command.CommandSampleStart) -> None:
    """
    Given a compliant sample request, issue the BigQuery corresponding sampling request.
    When finished, will push a Pub/Sub message containing the
        :py:class:`command.CommandSampleDone` request.

    :param cmd:
    :return:
    """
    _LOGGER.info('Issuing sample command <%s>', cmd)
    start_timestamp = int(time.time())
    error_message = ''
    sample_type = table.SortType.from_str(cmd.sample_request.sample.spec.type)
    kwargs = dict(
        source_table_ref=cmd.sample_request.table_reference,
        target_table_ref=cmd.target_table,
        amount=cmd.sample_request.sample.size.count,
        recreate_table=True,
    )
    if sample_type == table.SortType.RANDOM:
        sampler_query.create_table_with_random_sample(**kwargs)
    elif sample_type == table.SortType.SORTED:
        kwargs.update(
            dict(
                column=cmd.sample_request.sample.spec.properties.by,
                order=cmd.sample_request.sample.spec.properties.direction,
            )
        )
        sampler_query.create_table_with_sorted_sample(**kwargs)  # pylint: disable=missing-kwoa
    else:
        raise ValueError(f'Cannot process sample request of type <{sample_type}> in <{cmd}>')
    end_timestamp = int(time.time())
    sample_done = _create_sample_done_cmd(cmd, start_timestamp, end_timestamp, error_message)
    pubsub.publish(sample_done.as_dict(), _general_config().pubsub_request)


def _create_sample_done_cmd(
    cmd: command.CommandSampleStart,
    start_timestamp: int,
    end_timestamp: int,
    error_message: str,
) -> command.CommandSampleDone:
    kwargs = {
        command.CommandSampleDone.type.__name__: command.CommandType.SAMPLE_DONE.value,
        command.CommandSampleDone.timestamp.__name__: cmd.timestamp,
        command.CommandSampleDone.sample_request.__name__: cmd.sample_request,
        command.CommandSampleDone.target_table.__name__: cmd.target_table,
        command.CommandSampleDone.start_timestamp.__name__: start_timestamp,
        command.CommandSampleDone.end_timestamp.__name__: end_timestamp,
        command.CommandSampleDone.error_message.__name__: error_message,
    }
    return command.CommandSampleDone(**kwargs)


def _process_sample_done(cmd: command.CommandSampleDone) -> None:
    """
    Collect the signal that a given sampling request has finished, logging it.
    If there is any error message, pushes the message into the error Pub/Sub topic.

    :param cmd:
    :return:
    """
    _LOGGER.info('Completed sample for command <%s>', cmd)

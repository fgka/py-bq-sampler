# vim: ai:sw=4:ts=4:sta:et:fo=croql
"""
Processes a request coming from Cloud Function.
"""
import os
import time
from typing import Optional

import cachetools

from bq_sampler.entity import command, table, policy
from bq_sampler import sampler_bucket, sampler_query
from bq_sampler.gcp import gcs, pubsub
from bq_sampler import const, logger

_LOGGER = logger.get(__name__)

_BQ_LOCATION_ENV_VAR: str = 'BQ_LOCATION'  # europe-west3
_BQ_TARGET_PROJECT_ID_ENV_VAR: str = 'TARGET_PROJECT_ID'  # my-target-project-12345
_GCS_POLICY_BUCKET_ENV_VAR: str = 'POLICY_BUCKET_NAME'  # my-policy-bucket
_GCS_DEFAULT_POLICY_OBJECT_PATH_ENV_VAR: str = 'DEFAULT_POLICY_OBJECT_PATH'  # default_policy.json
_DEFAULT_GCS_DEFAULT_POLICY_OBJECT_PATH: str = 'default_policy.json'
_GCS_REQUEST_BUCKET_ENV_VAR: str = 'REQUEST_BUCKET_NAME'  # my-request-bucket
_PUBSUB_CMD_TOPIC_ENV_VAR: str = 'CMD_TOPIC_NAME'  # projects/py-project-12345/topics/cmd-topic-name
_PUBSUB_ERROR_TOPIC_ENV_VAR: str = (
    'ERROR_TOPIC_NAME'  # projects/py-project-12345/topics/error-topic-name
)
_SAMPLING_LOCK_OBJECT_PATH_ENV_VAR: str = 'SAMPLING_LOCK_OBJECT_PATH'  # block-sampling
_DEFAULT_SAMPLING_LOCK_OBJECT_PATH: str = 'block-sampling'

_PUBSUB_ERROR_CMD_ENTRY: str = 'command'
_PUBSUB_ERROR_MSG_ENTRY: str = 'error'


class _GeneralConfig:  # pylint: disable=too-many-instance-attributes
    def __init__(self):
        self._location = os.environ.get(_BQ_LOCATION_ENV_VAR)
        self._target_project_id = os.environ.get(_BQ_TARGET_PROJECT_ID_ENV_VAR)
        self._policy_bucket = os.environ.get(_GCS_POLICY_BUCKET_ENV_VAR)
        self._default_policy_path = os.environ.get(
            _GCS_DEFAULT_POLICY_OBJECT_PATH_ENV_VAR, _DEFAULT_GCS_DEFAULT_POLICY_OBJECT_PATH
        )
        self._request_bucket = os.environ.get(_GCS_REQUEST_BUCKET_ENV_VAR)
        self._pubsub_request = os.environ.get(_PUBSUB_CMD_TOPIC_ENV_VAR)
        self._pubsub_error = os.environ.get(_PUBSUB_ERROR_TOPIC_ENV_VAR)
        self._sampling_lock_path = os.environ.get(
            _SAMPLING_LOCK_OBJECT_PATH_ENV_VAR, _DEFAULT_SAMPLING_LOCK_OBJECT_PATH
        )

    @property
    def location(self) -> str:  # pylint: disable=missing-function-docstring
        return self._location

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

    @property
    def sampling_lock_path(self) -> str:  # pylint: disable=missing-function-docstring
        return self._sampling_lock_path


def process(value: command.CommandBase) -> None:
    """
    Single entry point to process commands coming from Pub/Sub.

    :param value:
    :return:
    """
    _LOGGER.info('Processing command <%s>', value)
    try:
        _process(value)
        _LOGGER.debug('Processed command <%s>', value)
    except Exception as err:
        error_data = {
            _PUBSUB_ERROR_CMD_ENTRY: value.as_dict(),
            _PUBSUB_ERROR_MSG_ENTRY: str(err),
        }
        pubsub.publish(error_data, _general_config().pubsub_error)
        raise RuntimeError(f'Could not process command: <{value}>. Error: {err}') from err


def _process(value: command.CommandBase) -> None:
    if value.type == command.CommandType.START.value:
        _process_start(value)
    elif value.type == command.CommandType.SAMPLE_POLICY_PREFIX.value:
        _process_sample_policy_prefix(value)
    elif value.type == command.CommandType.SAMPLE_START.value:
        _process_sample_start(value)
    elif value.type == command.CommandType.SAMPLE_DONE.value:
        _process_sample_done(value)
    else:
        raise ValueError(f'Command type <{value.type}> cannot be processed')


def _process_start(value: command.CommandStart) -> None:
    """
    Will inspect the policy and requests buckets,
        apply compliance,
        and issue a sampling request for all targeted tables.
    It will also generate a :py:class:`command.CommandSampleStart` for each one'
        and send it out into the Pub/Sub topic.

    :param value:
    :return:
    """
    if sampler_bucket.is_sampling_lock_present(
        _general_config().request_bucket, _general_config().sampling_lock_path
    ):
        gcs_url = f'gs://{_general_config().request_bucket}/{_general_config().sampling_lock_path}'
        _LOGGER.warning(
            'Sampling is being locked by object <%s>. Sampling is being skipped', gcs_url
        )
        raise InterruptedError(
            f'Sampling interrupted by presence of <{gcs_url}>. Remove to re-enable sampling.'
        )
    _process_start_ok(value)


def _process_start_ok(value: command.CommandStart) -> None:
    _LOGGER.debug('Dropping all sample tables in <%s>', _general_config().target_project_id)
    sampler_query.drop_all_sample_tables(
        project_id=_general_config().target_project_id, location=_general_config().location
    )

    def prefix_filter_fn(full_path: str) -> bool:
        return len(full_path.strip(const.GS_PREFIX_DELIM).split(const.GS_PREFIX_DELIM)) == 2

    for prefix in gcs.list_prefixes(
        bucket_name=_general_config().policy_bucket, filter_fn=prefix_filter_fn
    ):
        _LOGGER.debug('Sending request for prefix: %s', prefix)
        # create sample for prefix request event
        sample_policy_prefix_req = _create_sample_policy_prefix_cmd(value, prefix)
        # send request out
        _publish_cmd_to_pubsub(sample_policy_prefix_req)


def _create_sample_policy_prefix_cmd(
    value: command.CommandStart, prefix: str
) -> command.CommandSamplePolicyPrefix:
    # pylint: disable=line-too-long
    kwargs = {
        command.CommandSamplePolicyPrefix.type.__name__: command.CommandType.SAMPLE_POLICY_PREFIX.value,
        command.CommandSamplePolicyPrefix.timestamp.__name__: value.timestamp,
        command.CommandSamplePolicyPrefix.prefix.__name__: prefix,
    }
    # pylint: enable=line-too-long
    return command.CommandSamplePolicyPrefix(**kwargs)


def _process_sample_policy_prefix(value: command.CommandSamplePolicyPrefix) -> None:
    """
    Will list all policies in the policy bucket but restricted to the given prefix in `cmd`.
    For each policy will issue the corresponding :py:class:`command.CommandSampleStart`.

    :param value:
    :return:
    """
    _LOGGER.info('Issuing sample command <%s>', value)
    _process_sample_policy_prefix_ok(value)


def _process_sample_policy_prefix_ok(value: command.CommandSamplePolicyPrefix) -> None:
    _LOGGER.debug(
        'Retrieving all policies from bucket <%s> and prefix <%s>',
        _general_config().policy_bucket,
        value.prefix,
    )
    errors = []
    for table_policy in sampler_bucket.all_policies(
        bucket_name=_general_config().policy_bucket,
        default_policy_object_path=_general_config().default_policy_path,
        location=_general_config().location,
        prefix=value.prefix,
    ):
        _LOGGER.info(
            'Retrieving request, if existent, for table <%s> from bucket <%s> and prefix <%s>',
            table_policy,
            _general_config().request_bucket,
            value.prefix,
        )
        try:
            table_sample = sampler_bucket.sample_request_from_policy(
                bucket_name=_general_config().request_bucket,
                table_policy=table_policy,
            )
            # compliance enforcement
            table_sample = _compliant_sample_request(table_policy, table_sample)
            # create sample request event
            start_sample_req = _create_sample_start_cmd(value, table_policy, table_sample)
            # send request out
            _publish_cmd_to_pubsub(start_sample_req)
        except Exception as err:  # pylint: disable=broad-except
            msg = f'Ignoring sampling for policy {table_policy} due to error: {err}'
            errors.append(msg)
            _LOGGER.error(msg)
    if errors:
        raise RuntimeError(f'Failed command {value} with error(s): {errors}')


@cachetools.cached(cache=cachetools.LRUCache(maxsize=1))
def _general_config() -> _GeneralConfig:
    return _GeneralConfig()


def _compliant_sample_request(
    table_policy: policy.TablePolicy, table_sample: table.TableSample
) -> table.TableSample:
    row_count = sampler_query.row_count(table_sample.table_reference)
    return table_policy.compliant_sample(table_sample, row_count)


def _create_sample_start_cmd(
    value: command.CommandSamplePolicyPrefix,
    table_policy: policy.TablePolicy,
    table_sample: table.TableSample,
) -> command.CommandSampleStart:
    source_table = table_policy.table_reference
    kwargs = {
        command.CommandSampleStart.type.__name__: command.CommandType.SAMPLE_START.value,
        command.CommandSampleStart.timestamp.__name__: value.timestamp,
        command.CommandSampleStart.sample_request.__name__: table_sample,
        command.CommandSampleStart.target_table.__name__: source_table.clone(
            project_id=_general_config().target_project_id
        ),
    }
    return command.CommandSampleStart(**kwargs)


def _publish_cmd_to_pubsub(value: command.CommandBase) -> None:
    topic = _general_config().pubsub_request
    _LOGGER.debug('Sending event request <%s> to topic <%s>', value, topic)
    data = value.as_dict()
    pubsub.publish(data, topic)


def _process_sample_start(value: command.CommandSampleStart) -> None:
    """
    Given a compliant sample request, issue the BigQuery corresponding sampling request.
    When finished, will push a Pub/Sub message containing the
        :py:class:`command.CommandSampleDone` request.

    :param value:
    :return:
    """
    _LOGGER.info('Issuing sample command <%s>', value)
    start_timestamp = int(time.time())
    error_message = ''
    sample_type = table.SortType.from_str(value.sample_request.sample.spec.type)
    kwargs = dict(
        source_table_ref=value.sample_request.table_reference,
        target_table_ref=value.target_table,
        amount=value.sample_request.sample.size.count,
        recreate_table=True,
    )
    amount_inserted = 0
    if sample_type == table.SortType.RANDOM:
        amount_inserted = sampler_query.create_table_with_random_sample(**kwargs)
    elif sample_type == table.SortType.SORTED:
        kwargs.update(
            dict(
                column=value.sample_request.sample.spec.properties.by,
                order=value.sample_request.sample.spec.properties.direction,
            )
        )
        # pylint: disable=missing-kwoa
        amount_inserted = sampler_query.create_table_with_sorted_sample(**kwargs)
        # pylint: enable=missing-kwoa
    else:
        raise ValueError(f'Cannot process sample request of type <{sample_type}> in <{value}>')
    end_timestamp = int(time.time())
    sample_done = _create_sample_done_cmd(
        value, start_timestamp, end_timestamp, error_message, amount_inserted
    )
    pubsub.publish(sample_done.as_dict(), _general_config().pubsub_request)


def _create_sample_done_cmd(
    value: command.CommandSampleStart,
    start_timestamp: int,
    end_timestamp: int,
    error_message: str,
    amount_inserted: Optional[int] = None,
) -> command.CommandSampleDone:
    kwargs = {
        command.CommandSampleDone.type.__name__: command.CommandType.SAMPLE_DONE.value,
        command.CommandSampleDone.timestamp.__name__: value.timestamp,
        command.CommandSampleDone.sample_request.__name__: value.sample_request,
        command.CommandSampleDone.target_table.__name__: value.target_table,
        command.CommandSampleDone.start_timestamp.__name__: start_timestamp,
        command.CommandSampleDone.end_timestamp.__name__: end_timestamp,
        command.CommandSampleDone.error_message.__name__: error_message,
        command.CommandSampleDone.amount_inserted.__name__: amount_inserted,
    }
    return command.CommandSampleDone(**kwargs)


def _process_sample_done(value: command.CommandSampleDone) -> None:
    """
    Collect the signal that a given sampling request has finished, logging it.
    If there is any error message, pushes the message into the error Pub/Sub topic.

    :param value:
    :return:
    """
    _LOGGER.info('Completed sample for command <%s>', value)


if __name__ == '__main__':
    os.environ.setdefault(_BQ_LOCATION_ENV_VAR, 'europe-west3')
    os.environ.setdefault(_BQ_TARGET_PROJECT_ID_ENV_VAR, 'tgt-bq')
    os.environ.setdefault(_GCS_POLICY_BUCKET_ENV_VAR, 'test-list-blobs')
    os.environ.setdefault(_GCS_DEFAULT_POLICY_OBJECT_PATH_ENV_VAR, 'default-policy.json')
    os.environ.setdefault(_GCS_REQUEST_BUCKET_ENV_VAR, 'sample-request-831514123984')
    os.environ.setdefault(_PUBSUB_CMD_TOPIC_ENV_VAR, 'projects/src-bq/topics/bq-sampler-cmd')
    os.environ.setdefault(_PUBSUB_ERROR_TOPIC_ENV_VAR, 'projects/src-bq/topics/bq-sampler-err')
    cmd = command.CommandStart(type=command.CommandType.START.value, timestamp=1)
    process(cmd)

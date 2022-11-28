# vim: ai:sw=4:ts=4:sta:et:fo=croql
"""
Processes a request coming from Cloud Function.
"""
import os
import time
from typing import Any, Dict, Optional, Tuple

import cachetools
import tenacity

from bq_sampler.entity import command, table, policy
from bq_sampler import const, logger, sampler_bucket, sampler_query
from bq_sampler.gcp import bq, gcs, pubsub

_LOGGER = logger.get(__name__)

_BQ_TARGET_LOCATION_ENV_VAR: str = 'BQ_TARGET_LOCATION'  # europe-west3
_BQ_TRANSFER_NOTIFICATION_TOPIC_ENV_VAR: str = (
    'BQ_TRANSFER_NOTIFICATION_TOPIC'  # projects/py-project-12345/topics/bq-notification-topic-name
)
_BQ_TARGET_PROJECT_ID_ENV_VAR: str = 'TARGET_PROJECT_ID'  # my-2_target-project-12345
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
        self._target_location = os.environ.get(_BQ_TARGET_LOCATION_ENV_VAR)
        self._target_project_id = os.environ.get(_BQ_TARGET_PROJECT_ID_ENV_VAR)
        self._policy_bucket = os.environ.get(_GCS_POLICY_BUCKET_ENV_VAR)
        self._default_policy_path = os.environ.get(
            _GCS_DEFAULT_POLICY_OBJECT_PATH_ENV_VAR, _DEFAULT_GCS_DEFAULT_POLICY_OBJECT_PATH
        )
        self._request_bucket = os.environ.get(_GCS_REQUEST_BUCKET_ENV_VAR)
        self._pubsub_request = os.environ.get(_PUBSUB_CMD_TOPIC_ENV_VAR)
        self._pubsub_error = os.environ.get(_PUBSUB_ERROR_TOPIC_ENV_VAR)
        self._pubsub_bq_notification = os.environ.get(_BQ_TRANSFER_NOTIFICATION_TOPIC_ENV_VAR)
        self._sampling_lock_path = os.environ.get(
            _SAMPLING_LOCK_OBJECT_PATH_ENV_VAR, _DEFAULT_SAMPLING_LOCK_OBJECT_PATH
        )

    @property
    def target_location(self) -> str:  # pylint: disable=missing-function-docstring
        return self._target_location

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
    def pubsub_bq_notification(self) -> str:  # pylint: disable=missing-function-docstring
        return self._pubsub_bq_notification

    @property
    def sampling_lock_path(self) -> str:  # pylint: disable=missing-function-docstring
        return self._sampling_lock_path


def process(value: command.CommandBase, *, with_retry: Optional[bool] = True) -> str:
    """
    Single entry point to process commands coming from Pub/Sub.

    :param value:
    :param with_retry:
    :return:
    """
    _LOGGER.info('Processing command <%s> with retry to <%s>', value, with_retry)
    try:
        if with_retry:
            _process_with_retry(value)
        else:
            _process(value)
        _LOGGER.info('Processed command <%s>', value)
    except Exception as err:  # pylint: disable=broad-except
        error_data = {
            _PUBSUB_ERROR_CMD_ENTRY: value.as_dict(),
            _PUBSUB_ERROR_MSG_ENTRY: str(err),
        }
        pubsub.publish(error_data, _general_config().pubsub_error)
        _LOGGER.error('Sent error to %s. Message: %s', _general_config().pubsub_error, error_data)
        raise RuntimeError(f'Could not process command: <{value}>. Error: {err}') from err
    return 'OK'


@tenacity.retry(
    retry=tenacity.retry_if_not_exception_type(ValueError),
    wait=tenacity.wait_exponential(multiplier=1, min=4, max=10),
    stop=tenacity.stop_after_attempt(3),
)
def _process_with_retry(value: command.CommandBase) -> None:
    _process(value)


def _process(value: command.CommandBase) -> None:
    if value.type == command.CommandType.START.value:
        _process_start(value)
    elif value.type == command.CommandType.SAMPLE_POLICY_PREFIX.value:
        _process_sample_policy_prefix(value)
    elif value.type == command.CommandType.SAMPLE_START.value:
        _process_sample_start(value)
    elif value.type == command.CommandType.SAMPLE_DONE.value:
        _process_sample_done(value)
    elif value.type == command.CommandType.TRANSFER_RUN_DONE.value:
        _process_transfer_run_done(value)
    elif value.type == command.CommandType.REMOVE_DATASET.value:
        _process_remove_dataset(value)
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
    _clean_up_project_before_start(
        project_id=_general_config().target_project_id, location=_general_config().target_location
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


def _clean_up_project_before_start(project_id: str, location: str) -> None:
    _LOGGER.debug('Cleaning up before start targeting project <%s>', project_id)
    sampler_query.drop_all_sample_tables(project_id=project_id)
    sampler_query.remove_all_empty_sample_datasets(project_id=project_id)
    sampler_query.remove_all_transfer_config(project_id=project_id, location=location)
    _LOGGER.info('Project <%s> cleaned up for start', project_id)


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
            start_sample_req = _create_sample_start_cmd(
                value, table_policy, table_sample, _general_config().target_location
            )
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
    target_location: Optional[str] = None,
) -> command.CommandSampleStart:
    source_table = table_policy.table_reference
    kwargs = {
        command.CommandSampleStart.type.__name__: command.CommandType.SAMPLE_START.value,
        command.CommandSampleStart.timestamp.__name__: value.timestamp,
        command.CommandSampleStart.sample_request.__name__: table_sample,
        command.CommandSampleStart.target_table.__name__: source_table.clone(
            project_id=_general_config().target_project_id,
            location=target_location,
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
        notification_pubsub_topic=_general_config().pubsub_bq_notification,
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


def _process_transfer_run_done(value: command.CommandTransferRunDone) -> None:
    # pylint: disable=line-too-long
    """
    Checks the state and, if successful, removes the transfer config and 3_source dataset.

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

    :param value:
    :return:
    """
    # pylint: enable=line-too-long
    _LOGGER.info('Processing transfer run completion <%s>', value)
    # validate state
    state = value.payload.get(const.TRANSFER_RUN_STATE_ATTR)
    if state != const.TRANSFER_RUN_STATE_SUCCEEDED_VALUE:
        raise RuntimeError(
            f'Transfer Run did not succeeded (state <{state}>), therefore not removing'
        )
    # remove transfer config
    bq.remove_transfer_config(value.name)
    # send remove dataset command
    project_id, dataset_id = _extract_source_dataset_from_transfer_run_payload(value.payload)
    remove_dataset = _create_remove_dataset_cmd(project_id, dataset_id, value.timestamp)
    pubsub.publish(remove_dataset.as_dict(), _general_config().pubsub_request)


def _extract_source_dataset_from_transfer_run_payload(payload: Dict[str, Any]) -> Tuple[str, str]:
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

    :param payload:
    :return:
    """
    # pylint: enable=line-too-long
    project_id = None
    dataset_id = None
    params = payload.get(const.TRANSFER_RUN_PARAMS_ATTR)
    if isinstance(params, dict):
        project_id = params.get(const.TRANSFER_RUN_PARAMS_SOURCE_PROJECT_ID_ATTR)
        dataset_id = params.get(const.TRANSFER_RUN_PARAMS_SOURCE_DATASET_ID_ATTR)
    return project_id, dataset_id


def _create_remove_dataset_cmd(
    project_id: str, dataset_id: str, timestamp: int
) -> command.CommandRemoveDataset:
    kwargs = {
        command.CommandRemoveDataset.type.__name__: command.CommandType.REMOVE_DATASET.value,
        command.CommandRemoveDataset.timestamp.__name__: timestamp,
        command.CommandRemoveDataset.project_id.__name__: project_id,
        command.CommandRemoveDataset.dataset_id.__name__: dataset_id,
    }
    return command.CommandRemoveDataset(**kwargs)


def _process_remove_dataset(value: command.CommandRemoveDataset) -> None:
    """
    Removes a dataset

    :param value:
    :return:
    """
    _LOGGER.info('Removing dataset <%s>', value)
    bq.remove_dataset(
        project_id=value.project_id, dataset_id=value.dataset_id, delete_contents=True
    )


def _process_sample_done(value: command.CommandSampleDone) -> None:
    """
    Collect the signal that a given sampling request has finished, logging it.
    If there is any error message, pushes the message into the error Pub/Sub topic.

    :param value:
    :return:
    """
    _LOGGER.info('Completed sample for command <%s>', value)


if __name__ == '__main__':
    os.environ.setdefault(_BQ_TARGET_LOCATION_ENV_VAR, 'europe-west3')
    os.environ.setdefault(_BQ_TARGET_PROJECT_ID_ENV_VAR, 'tgt-bq')
    os.environ.setdefault(_GCS_POLICY_BUCKET_ENV_VAR, 'test-list-blobs')
    os.environ.setdefault(_GCS_DEFAULT_POLICY_OBJECT_PATH_ENV_VAR, 'default-policy.json')
    os.environ.setdefault(_GCS_REQUEST_BUCKET_ENV_VAR, 'sample-request-831514123984')
    os.environ.setdefault(_PUBSUB_CMD_TOPIC_ENV_VAR, 'projects/src-bq/topics/bq-sampler-cmd')
    os.environ.setdefault(_PUBSUB_ERROR_TOPIC_ENV_VAR, 'projects/src-bq/topics/bq-sampler-err')
    cmd = command.CommandStart(type=command.CommandType.START.value, timestamp=1)
    process(cmd)

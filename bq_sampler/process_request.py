# vim: ai:sw=4:ts=4:sta:et:fo=croql
"""
Processes a request coming from Cloud Function.
"""
import logging
import os
import time

import cachetools

from bq_sampler.entity import request, table, policy
from bq_sampler import sampler_bucket, sampler_query
from bq_sampler.gcp import pubsub

_LOGGER = logging.getLogger(__name__)

_BQ_TARGET_PROJECT_ID_ENV_VAR: str = 'TARGET_PROJECT_ID'
_GCS_POLICY_BUCKET_ENV_VAR: str = 'POLICY_BUCKET_NAME'
_GCS_DEFAULT_POLICY_OBJECT_PATH_ENV_VAR: str = 'DEFAULT_POLICY_OBJECT_PATH'
_GCS_REQUEST_BUCKET_ENV_VAR: str = 'REQUEST_BUCKET_NAME'
_PUBSUB_REQUEST_TOPIC_ENV_VAR: str = 'REQUEST_TOPIC_NAME'
_PUBSUB_ERROR_TOPIC_ENV_VAR: str = 'ERROR_TOPIC_NAME'


class _GeneralConfig:
    def __init__(self):
        self._target_project_id = os.environ.get(_BQ_TARGET_PROJECT_ID_ENV_VAR)
        self._policy_bucket = os.environ.get(_GCS_POLICY_BUCKET_ENV_VAR)
        self._default_policy_path = os.environ.get(_GCS_DEFAULT_POLICY_OBJECT_PATH_ENV_VAR)
        self._request_bucket = os.environ.get(_GCS_REQUEST_BUCKET_ENV_VAR)
        self._pubsub_request = os.environ.get(_PUBSUB_REQUEST_TOPIC_ENV_VAR)
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


def process(event_request: request.EventRequest) -> None:
    """
    Single entry point to process requests coming from Pub/Sub.

    :param event_request:
    :param project_id:
    :return:
    """
    _LOGGER.debug('Processing request <%s>', event_request)
    try:
        _process(event_request)
        _LOGGER.debug('Processed request <%s>', event_request)
    except Exception as err:
        error_data = {
            'event': event_request.as_dict(),
            'error': str(err),
        }
        pubsub.publish(error_data, _general_config().pubsub_error)
        msg = f'Could not process event: <{event_request}>. Error: {err}'
        _LOGGER.critical(msg)
        raise RuntimeError(msg) from err


def _process(event_request: request.EventRequest) -> None:
    if event_request.type == request.RequestType.START.value:
        _process_start(event_request)
    elif event_request.type == request.RequestType.SAMPLE_START.value:
        _process_sample_start(event_request)
    elif event_request.type == request.RequestType.SAMPLE_DONE.value:
        _process_sample_done(event_request)
    else:
        raise ValueError(f'Event request type <{event_request.type}> cannot be processed')


def _process_start(event_request: request.EventRequestStart) -> None:
    """
    Will inspect the policy and requests buckets,
        apply compliance,
        and issue a sampling request for all targeted tables.
    It will also generate a :py:class:`request.EventRequestSampleStart` for each one
        and send it out into the Pub/Sub topic.

    :param event_request:
    :param project_id:
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
        start_sample_req = _create_sample_start_request(event_request, table_policy, table_sample)
        # send request out
        _publish_event_request_to_pubsub(start_sample_req)


@cachetools.cached(cache=cachetools.LRUCache(maxsize=1))
def _general_config() -> _GeneralConfig:
    return _GeneralConfig()


def _compliant_sample_request(
    table_policy: policy.TablePolicy, table_sample: table.TableSample
) -> table.TableSample:
    row_count = sampler_query.row_count(table_sample.table_reference)
    return table_policy.compliant_sample(table_sample, row_count)


def _create_sample_start_request(
    event_request: request.EventRequestStart,
    table_policy: policy.TablePolicy,
    table_sample: table.TableSample,
) -> request.EventRequestSampleStart:
    source_table = table_policy.table_reference
    kwargs = {
        request.EventRequestSampleStart.type.__name__: request.RequestType.SAMPLE_START.value,
        request.EventRequestSampleStart.request_timestamp.__name__: event_request.request_timestamp,
        request.EventRequestSampleStart.sample_request.__name__: table_sample,
        request.EventRequestSampleStart.target_table.__name__: source_table.clone(
            project_id=_general_config().target_project_id
        ),
    }
    return request.EventRequestSampleStart(**kwargs)


def _publish_event_request_to_pubsub(event_request: request.EventRequest) -> None:
    topic = _general_config().pubsub_request
    _LOGGER.debug('Sending event request <%s> to topic <%s>', event_request, topic)
    data = event_request.as_dict()
    pubsub.publish(data, topic)


def _process_sample_start(event_request: request.EventRequestSampleStart) -> None:
    """
    Given a compliant sample request, issue the BigQuery corresponding sampling request.
    When finished, will push a Pub/Sub message containing the
        :py:class:`request.EventRequestSampleDone` request.

    :param event_request:
    :return:
    """
    _LOGGER.info('Issuing sample request <%s>', event_request)
    start_timestamp = int(time.time())
    error_message = ''
    sample_type = table.SortType.from_str(event_request.sample_request.sample.spec.type)
    kwargs = dict(
        source_table_ref=event_request.sample_request.table_reference,
        target_table_ref=event_request.target_table,
        amount=event_request.sample_request.sample.size.count,
        recreate_table=True,
    )
    if sample_type == table.SortType.RANDOM:
        sampler_query.create_table_with_random_sample(**kwargs)
    elif sample_type == table.SortType.SORTED:
        kwargs.update(
            dict(
                column=event_request.sample_request.sample.spec.properties.by,
                order=event_request.sample_request.sample.spec.properties.direction,
            )
        )
        sampler_query.create_table_with_sorted_sample(**kwargs)  # pylint: disable=missing-kwoa
    else:
        raise ValueError(
            f'Cannot process sample request of type <{sample_type}> in <{event_request}>'
        )
    end_timestamp = int(time.time())
    sample_done = _create_sample_done_request(
        event_request, start_timestamp, end_timestamp, error_message
    )
    pubsub.publish(sample_done.as_dict(), _general_config().pubsub_request)


def _create_sample_done_request(
    event_request: request.EventRequestSampleStart,
    start_timestamp: int,
    end_timestamp: int,
    error_message: str,
) -> request.EventRequestSampleDone:
    kwargs = {
        request.EventRequestSampleDone.type.__name__: request.RequestType.SAMPLE_DONE.value,
        request.EventRequestSampleDone.request_timestamp.__name__: event_request.request_timestamp,
        request.EventRequestSampleDone.sample_request.__name__: event_request.sample_request,
        request.EventRequestSampleDone.target_table.__name__: event_request.target_table,
        request.EventRequestSampleDone.start_timestamp.__name__: start_timestamp,
        request.EventRequestSampleDone.end_timestamp.__name__: end_timestamp,
        request.EventRequestSampleDone.error_message.__name__: error_message,
    }
    return request.EventRequestSampleDone(**kwargs)


def _process_sample_done(event_request: request.EventRequestSampleDone) -> None:
    """
    Collect the signal that a given sampling request has finished, logging it.
    If there is any error message, pushes the message into the error Pub/Sub topic.

    :param event_request:
    :return:
    """
    _LOGGER.info('Completed sample for request <%s>', event_request)

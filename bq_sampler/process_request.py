# vim: ai:sw=4:ts=4:sta:et:fo=croql
"""
Processes a request coming from Cloud Function.
"""
import logging
import os

from typing import Any, Dict

import cachetools

from bq_sampler.dto import request, sample
from bq_sampler import sampler_bucket, sampler_query

_LOGGER = logging.getLogger(__name__)

_GCS_POLICY_BUCKET_ENV_VAR: str = 'POLICY_BUCKET_NAME'
_GCS_DEFAULT_POLICY_OBJECT_PATH_ENV_VAR: str = 'DEFAULT_POLICY_OBJECT_PATH'
_GCS_REQUEST_BUCKET_ENV_VAR: str = 'REQUEST_BUCKET_NAME'
_PUBSUB_REQUEST_TOPIC_ENV_VAR: str = 'REQUEST_TOPIC_NAME'
_PUBSUB_ERROR_TOPIC_ENV_VAR: str = 'ERROR_TOPIC_NAME'


class _GeneralConfig:
    def __init__(self):
        self._policy_bucket = os.environ.get(_GCS_POLICY_BUCKET_ENV_VAR)
        self._default_policy_path = os.environ.get(_GCS_DEFAULT_POLICY_OBJECT_PATH_ENV_VAR)
        self._request_bucket = os.environ.get(_GCS_REQUEST_BUCKET_ENV_VAR)
        self._pubsub_request = os.environ.get(_PUBSUB_REQUEST_TOPIC_ENV_VAR)
        self._pubsub_error = os.environ.get(_PUBSUB_ERROR_TOPIC_ENV_VAR)
        self._project_id = os.environ.get('TODO')

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


def process(event_request: request.EventRequest, project_id: str) -> None:
    """
    Single entry point to process requests coming from Pub/Sub.

    :param event_request:
    :param project_id:
    :return:
    """
    _LOGGER.info('Processing request <%s>', event_request)
    try:
        _process(event_request, project_id)
    except Exception as err:
        error_data = {
            'event': event_request.to_dict(),
            'error': err,
        }
        _publish_to_pubsub(_general_config().pubsub_error, error_data)
        msg = f'Could not process event: <{event_request}>. Error: {err}'
        _LOGGER.critical(msg)
        raise RuntimeError(msg) from err


def _process(event_request: request.EventRequest, project_id: str) -> None:
    if event_request.type == request.RequestType.START.value:
        _process_start(event_request, project_id)
    elif event_request.type == request.RequestType.SAMPLE_START.value:
        _process_sample_start(event_request)
    elif event_request.type == request.RequestType.SAMPLE_DONE.value:
        _process_sample_done(event_request)
    else:
        raise ValueError(f'Event request type <{event_request.type}> cannot be processed')


def _process_start(event_request: request.EventRequestStart, project_id: str) -> None:
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
    _LOGGER.info('Retrieving all policies from bucket <%s>', _general_config().policy_bucket)
    for table_policy in sampler_bucket.all_policies(
        bucket_name=_general_config().policy_bucket,
        default_policy_object_path=_general_config().default_policy_path,
    ):
        _LOGGER.info(
            'Retrieving request, if existent, for table <%s> from bucket <%s>',
            table_policy,
            _general_config().request_bucket,
        )
        sample_req = sampler_bucket.sample_request_from_policy(
            bucket_name=_general_config().request_bucket,
            table_policy=table_policy,
            sample_project_id=project_id,
        )
        # TODO compliance
        # TODO count
        event_dict = event_request.as_dict()
        event_dict['sample_request'] = sample_req.as_dict()
        event_dict['source_table'] = table_policy.table_reference.as_dict()
        event_sample_req = request.EventRequestSampleStart.from_dict(event_dict)
        _publish_event_request_to_pubsub(event_sample_req)


@cachetools.cached(cache=cachetools.LRUCache(maxsize=1))
def _general_config() -> _GeneralConfig:
    return _GeneralConfig()


def _publish_event_request_to_pubsub(event_request: request.EventRequest) -> None:
    topic = _general_config().pubsub_request
    _LOGGER.info('Sending event request <%s> to topic <%s>', event_request, topic)
    data = event_request.to_dict()
    # TODO
    _publish_to_pubsub(topic, data)


def _publish_to_pubsub(topic: str, data: Dict[str, Any]) -> None:
    raise NotImplementedError()


def _process_sample_start(event_request: request.EventRequestSampleStart) -> None:
    """
    Given a compliant sample request, issue the BigQuery corresponding sampling request.
    When finished, will push a Pub/Sub message containing the
        :py:class:`request.EventRequestSampleDone` request.

    :param event_request:
    :return:
    """
    _LOGGER.info('Issuing sample for request <%s>', event_request)
    sample_type = sample.SortType.from_str(event_request.sample_request.sample.spec.type)
    if sample_type == sample.SortType.RANDOM:
        sampler_query.create_table_with_random_sample(
            source_table_ref=event_request.source_table,
            target_table_ref=event_request.sample_request.table_reference,
            amount=event_request.sample_request.size.count,
            recreate_table=True,
        )
    elif sample_type == sample.SortType.SORTED:
        sampler_query.create_table_with_sorted_sample(
            source_table_ref=event_request.source_table,
            target_table_ref=event_request.sample_request.table_reference,
            amount=event_request.sample_request.size.count,
            column=event_request.sample_request.spec.properties.by,
            order=event_request.sample_request.spec.properties.direction,
            recreate_table=True,
        )
    else:
        raise ValueError(
            f'Cannot process sample request of type <{sample_type}> in <{event_request}>'
        )


def _process_sample_done(event_request: request.EventRequestSampleDone) -> None:
    """
    Collect the signal that a given sampling request has finished, logging it.
    If there is any error message, pushes the message into the error Pub/Sub topic.

    :param event_request:
    :return:
    """
    _LOGGER.info('Completed sample for request <%s>', event_request)

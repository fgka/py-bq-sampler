# vim: ai:sw=4:ts=4:sta:et:fo=croql
"""
Manages PubSub boilerplate. How to parse the data and publish it.
* https://cloud.google.com/pubsub/docs/push
* https://cloud.google.com/pubsub/docs/publisher
"""
import base64
from concurrent import futures
import json
from typing import Any, Dict, Union

import cachetools

from google.cloud import pubsub_v1

from bq_sampler import logger

_LOGGER = logger.get(__name__)


def parse_json_data(value: Union[str, bytes]) -> Any:
    """
    Parses a Pub/Sub base64 JSON coded :py:class:`str`.

    :param value: the raw payload from Pub/Sub.
    :return:
    """
    # parse PubSub payload
    try:
        result = json.loads(parse_str_data(value))
    except Exception as err:
        raise RuntimeError(
            f'Could not parse PubSub JSON data. Raw data: <{value}>. Error: {err}'
        ) from err
    return result


def parse_str_data(value: Union[str, bytes]) -> str:
    """
    Parses a Pub/Sub base64 coded :py:class:`str`.

    :param value: the raw payload from Pub/Sub.
    :return:
    """
    # parse PubSub payload
    if not isinstance(value, (bytes, str)):
        raise TypeError(
            f'Event data is not a {str.__name__} or {bytes.__name__}. Got: <{value}>({type(value)})'
        )
    try:
        result = base64.b64decode(value).decode('utf-8')
    except Exception as err:
        raise RuntimeError(
            f'Could not parse PubSub string data. Raw data: <{value}>. Error: {err}'
        ) from err
    return result


def publish(value: Dict[str, Any], topic_path: str) -> None:
    """
    Converts argument to a string to be published to a Pub/Sub topic.

    :param value:
    :param topic_path:
    :return:
    """
    # validate input
    if not isinstance(value, dict):
        raise TypeError(f'Value must be a {dict.__name__}. Got <{value}>({type(value)})')
    if not isinstance(topic_path, str) or not topic_path.strip():
        raise TypeError(
            f'Topic path must be a non-empty string. Got <{topic_path}>({type(topic_path)})'
        )
    # logic
    _LOGGER.debug('Publishing data <%s> into topic <%s>', value, topic_path)
    json_str = json.dumps(value)
    data = json_str.encode('utf-8')
    publish_future = _client().publish(topic_path, data)
    futures.wait([publish_future], return_when=futures.ALL_COMPLETED)
    _LOGGER.debug('Published data <%s> into topic <%s>', value, topic_path)


@cachetools.cached(cache=cachetools.LRUCache(maxsize=1))
def _client() -> pubsub_v1.PublisherClient:
    return pubsub_v1.PublisherClient()

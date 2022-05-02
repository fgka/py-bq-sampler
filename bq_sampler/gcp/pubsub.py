# vim: ai:sw=4:ts=4:sta:et:fo=croql
"""
Manages PubSub boilerplate. How to parse the data and publish it.
* https://cloud.google.com/pubsub/docs/push
* https://cloud.google.com/pubsub/docs/publisher
"""
import base64
from concurrent import futures
import json
import logging
from typing import Any, Dict, Union

import cachetools

from google.cloud import pubsub_v1

_LOGGER = logging.getLogger(__name__)


def parse_json_data(value: Union[str, bytes]) -> Any:
    """
    Parses a Pub/Sub base64 JSON coded :py:class:`str`.

    :param value: the raw payload from Pub/Sub.
    :return:
    """
    # parse PubSub payload
    if not isinstance(value, (bytes, str)):
        raise TypeError(
            f'Event data is not a {str.__name__} or {bytes.__name__}. Got: <{value}>({type(value)})'
        )
    try:
        decoded_event_data = base64.b64decode(value).decode('utf-8')
        result = json.loads(decoded_event_data)
    except Exception as err:
        msg = f'Could not parse PubSub data. Raw data: <{value}>. Error: {err}'
        _LOGGER.critical(msg)
        raise RuntimeError(msg) from err
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
    publish_future = _client().publish(data, topic_path)
    futures.wait([publish_future], return_when=futures.ALL_COMPLETED)
    _LOGGER.debug('Published data <%s> into topic <%s>', value, topic_path)


@cachetools.cached(cache=cachetools.LRUCache(maxsize=1))
def _client() -> pubsub_v1.PublisherClient:
    return pubsub_v1.PublisherClient()

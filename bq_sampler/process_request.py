# vim: ai:sw=4:ts=4:sta:et:fo=croql
"""
Processes a request coming from Cloud Function.
"""
import logging

from bq_sampler.dto import request

_LOGGER = logging.getLogger(__name__)


def process(event_request: request.EventRequest) -> None:
    _LOGGER.info('Processing request <%s>', event_request)

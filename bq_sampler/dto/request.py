# vim: ai:sw=4:ts=4:sta:et:fo=croql
"""
DTOs to encode a request coming from the Cloud Function.
"""
import attrs

from bq_sampler import const
from bq_sampler.dto import attrs_defaults


@attrs.define(**const.ATTRS_DEFAULTS)
class EventRequest(attrs_defaults.HasFromDict):  # pylint: disable=too-few-public-methods
    pass

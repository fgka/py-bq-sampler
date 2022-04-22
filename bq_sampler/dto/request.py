# vim: ai:sw=4:ts=4:sta:et:fo=croql
"""
DTOs to encode a request coming from the Cloud Function.
"""
from typing import Any

import attrs

from bq_sampler import const
from bq_sampler.dto import attrs_defaults
from bq_sampler.dto import sample


class RequestType(attrs_defaults.EnumWithFromStrIgnoreCase):
    """
    Available request types.
    """

    START = const.REQUEST_TYPE_START
    SAMPLE_START = const.REQUEST_TYPE_SAMPLE_START
    SAMPLE_DONE = const.REQUEST_TYPE_SAMPLE_DONE


@attrs.define(**const.ATTRS_DEFAULTS)
class EventRequest(attrs_defaults.HasFromDict):  # pylint: disable=too-few-public-methods
    """
    Common request DTO with mandatory fields
    """

    type: str = attrs.field(validator=attrs.validators.instance_of(str))
    request_timestamp: int = attrs.field(validator=attrs.validators.gt(0))

    @type.validator
    def _is_type_valid(  # pylint: disable=no-self-use
        self, attribute: attrs.Attribute, value: Any
    ) -> None:
        if not RequestType.from_str(value):
            raise ValueError(
                f'Attribute <{attribute.name}> must be one of {[d.value for d in RequestType]},'
                f' got: <{value}>'
            )


@attrs.define(**const.ATTRS_DEFAULTS)
class EventRequestStart(EventRequest):  # pylint: disable=too-few-public-methods
    """
    To represent a request to start the sampling.
    This is the trigger of it all.
    """


@attrs.define(**const.ATTRS_DEFAULTS)
class EventRequestSampleStart(EventRequest):  # pylint: disable=too-few-public-methods
    """
    To request the sampling of a specific table.
    """

    source_table: sample.TableReference = attrs.field(
        validator=attrs.validators.instance_of(sample.TableReference)
    )
    sample_request: sample.TableSample = attrs.field(
        validator=attrs.validators.instance_of(sample.TableSample)
    )


@attrs.define(**const.ATTRS_DEFAULTS)
class EventRequestSampleDone(EventRequestSampleStart):  # pylint: disable=too-few-public-methods
    """
    A signal to indicate that the sampling has been finished for a specific table.
    """

    start_timestamp: int = attrs.field(validator=attrs.validators.gt(0))
    end_timestamp: int = attrs.field(validator=attrs.validators.gt(0))
    error_message: str = attrs.field(validator=attrs.validators.instance_of(str))

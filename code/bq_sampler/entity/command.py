# vim: ai:sw=4:ts=4:sta:et:fo=croql
"""
DTOs to encode a command coming from the Cloud Function.
"""
from typing import Any

import attrs

from bq_sampler import const
from bq_sampler.entity import attrs_defaults
from bq_sampler.entity import table


class CommandType(attrs_defaults.EnumWithFromStrIgnoreCase):
    """
    Available command types.
    """

    START = const.REQUEST_TYPE_START
    SAMPLE_POLICY_PREFIX = const.REQUEST_TYPE_SAMPLE_POLICY_PREFIX
    SAMPLE_START = const.REQUEST_TYPE_SAMPLE_START
    SAMPLE_DONE = const.REQUEST_TYPE_SAMPLE_DONE


@attrs.define(**const.ATTRS_DEFAULTS)
class CommandBase(attrs_defaults.HasFromDict):  # pylint: disable=too-few-public-methods
    """
    Common command DTO with mandatory fields.
    """

    type: str = attrs.field(validator=attrs.validators.instance_of(str))
    timestamp: int = attrs.field(validator=attrs.validators.gt(0))

    @type.validator
    def _is_type_valid(  # pylint: disable=no-self-use
        self, attribute: attrs.Attribute, value: Any
    ) -> None:
        if not CommandType.from_str(value):
            raise ValueError(
                f'Attribute <{attribute.name}> must be one of {[d.value for d in CommandType]},'
                f' got: <{value}>'
            )


@attrs.define(**const.ATTRS_DEFAULTS)
class CommandStart(CommandBase):  # pylint: disable=too-few-public-methods
    """
    To represent a command to start the sampling.
    This is the trigger of it all.
    """


@attrs.define(**const.ATTRS_DEFAULTS)
class CommandSamplePolicyPrefix(CommandBase):  # pylint: disable=too-few-public-methods
    """
    A signal to indicate that a specific GCS policy bucket prefix will be processed.
    """

    prefix: str = attrs.field(validator=attrs.validators.instance_of(str))


@attrs.define(**const.ATTRS_DEFAULTS)
class CommandSampleStart(CommandBase):  # pylint: disable=too-few-public-methods
    """
    To issue the sampling of a specific table.
    """

    sample_request: table.TableSample = attrs.field(
        validator=attrs.validators.instance_of(table.TableSample)
    )
    target_table: table.TableReference = attrs.field(
        validator=attrs.validators.instance_of(table.TableReference)
    )


@attrs.define(**const.ATTRS_DEFAULTS)
class CommandSampleDone(CommandSampleStart):  # pylint: disable=too-few-public-methods
    """
    A signal to indicate that the sampling has been finished for a specific table.
    """

    start_timestamp: int = attrs.field(validator=attrs.validators.gt(0))
    end_timestamp: int = attrs.field(validator=attrs.validators.gt(0))
    error_message: str = attrs.field(default='', validator=attrs.validators.instance_of(str))
    amount_inserted: int = attrs.field(
        default=None, validator=attrs.validators.optional(attrs.validators.ge(0))
    )

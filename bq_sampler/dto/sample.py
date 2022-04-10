# vim: ai:sw=4:ts=4:sta:et:fo=croql
"""
DTOs to encode the sample request.
"""
from typing import Any

import attrs

from bq_sampler.dto import attrs_defaults


@attrs.define(**attrs_defaults.ATTRS_DEFAULTS)
class SampleSize(attrs_defaults.HasFromDict):  # pylint: disable=too-few-public-methods
    """
    Sample size as in::
        size = {
            "count": 123,
            "percentage": 10.1,
        }
    """

    count: int = attrs.field(
        default=None, validator=attrs.validators.optional(validator=attrs.validators.gt(0))
    )
    percentage: float = attrs.field(
        default=None,
        validator=attrs.validators.optional(
            validator=[attrs.validators.gt(0.0), attrs.validators.le(100.0)]
        ),
    )


class SortDirection(attrs_defaults.EnumWithFromStrIgnoreCase):
    """
    Available sorting directions.
    """

    ASC = "ASC"
    DESC = "DESC"


@attrs.define(**attrs_defaults.ATTRS_DEFAULTS)
class _SortProperties(attrs_defaults.HasFromDict):  # pylint: disable=too-few-public-methods
    """
    DTO for sort properties as in::
        sort_properties = {
            "by": "my_column",
            "direction": "DESC"
        }
    """

    by: str = attrs.field(  # pylint: disable=invalid-name
        validator=attrs.validators.instance_of(str)
    )
    direction: str = attrs.field(validator=attrs.validators.instance_of(str))

    @by.validator
    def _is_by_valid(  # pylint: disable=no-self-use
        self, attribute: attrs.Attribute, value: Any
    ) -> None:
        if not value or value.strip() != value:
            raise ValueError(
                f'Attribute <{attribute.name}> must be a non-empty stripped string, got: <{value}>'
            )

    @direction.validator
    def _is_direction_valid(  # pylint: disable=no-self-use
        self, attribute: attrs.Attribute, value: Any
    ) -> None:
        if not SortDirection.from_str(value):
            raise ValueError(
                f'Attribute <{attribute.name}> must be one of {[d.value for d in SortDirection]},'
                f' got: <{value}>'
            )


class SortType(attrs_defaults.EnumWithFromStrIgnoreCase):
    """
    Which type of sorting is supported in the sample.
    """

    RANDOM = "random"
    SORTED = "sorted"

    @classmethod
    def default(cls) -> Any:
        """
        Returns the default sorting strategy.

        :return:
        """
        return SortType.RANDOM


@attrs.define(**attrs_defaults.ATTRS_DEFAULTS)
class SortAlgorithm(attrs_defaults.HasFromDict):  # pylint: disable=too-few-public-methods
    """
    DTO for the sort algorithm as in::
        sort_algorithm = {
            "type": "sorted",
            "properties": {
                "by": "my_column",
                "direction": "DESC"
            }
        }
    """

    type: str = attrs.field(
        default=SortType.default(),
        validator=attrs.validators.optional(validator=attrs.validators.instance_of(str)),
    )
    properties: _SortProperties = attrs.field(
        default=None,
        validator=attrs.validators.optional(
            validator=attrs.validators.instance_of(_SortProperties)
        ),
    )

    @type.validator
    def _is_type_valid(  # pylint: disable=no-self-use
        self, attribute: attrs.Attribute, value: Any
    ) -> None:
        if value is not None and SortType.from_str(value) is None:
            raise ValueError(
                f'Attribute <{attribute.name}> needs to be one of {[t.value for t in SortType]},'
                f' got: <{value}>'
            )


@attrs.define(**attrs_defaults.ATTRS_DEFAULTS)
class Sample(attrs_defaults.HasFromJsonString):  # pylint: disable=too-few-public-methods
    """
    DTO for a sample definition as in::
        sample = {
            "size": {
                "count": 123,
                "percentage": 19.2
            },
            "spec": {
                "type": "sorted",
                "properties": {
                    "by": "my_column",
                    "direction": "DESC"
                }
            }
        }
    """

    size: SampleSize = attrs.field(
        default=None,
        validator=attrs.validators.optional(validator=attrs.validators.instance_of(SampleSize)),
    )
    spec: SortAlgorithm = attrs.field(
        default=None,
        validator=attrs.validators.optional(validator=attrs.validators.instance_of(SortAlgorithm)),
    )

    def patch_is_substitution(self) -> bool:
        """
        The rationale for using merge strategy is because a :py:class:`Sample`
        will only be patched when it is cascading from policies.

        This means:
        - Specific policy's `default_sample` will be patched with default policy's `default_sample`;
        - A request will be patched by the patched specific policy.

        Example::
            default_policy = {
                "size": {
                    "count": 1000
                },
                "spec": {
                    "type": "random",
                }
            }
            specific_policy = {
                "size": {
                    "percentage": 10.0
                }
            }
            # The policy against which the request will be patched
            # uses default's `spec` but specific's `size`
            patched_specific_policy = {
                "size": {
                    "percentage": 10.0
                },
                "spec": {
                    "type": "random"
                }
            }
            # The user request
            sample_request = {
                "spec": {
                    "type": "sorted",
                    "spec": {
                        "by": "my_column",
                        "direction": "ASC"
                    }
                }
            }
            # The actual request to be processed
            # uses request's `spec` but policy's `size`
            patched_request = {
                "size": {
                    "percentage": 10.0
                },
                "spec": {
                    "type": "sorted",
                    "spec": {
                        "by": "my_column",
                        "direction": "ASC"
                    }
                }
            }

        :return:
        """
        return False


@attrs.define(**attrs_defaults.ATTRS_DEFAULTS)
class TableReference(attrs_defaults.HasFromDict):  # pylint: disable=too-few-public-methods
    """
    DTO to fully specify a table location
    """

    project_id: str = attrs.field(validator=attrs.validators.instance_of(str))
    dataset_id: str = attrs.field(validator=attrs.validators.instance_of(str))
    table_id: str = attrs.field(validator=attrs.validators.instance_of(str))
    region: str = attrs.field(
        default=None,
        validator=attrs.validators.optional(validator=attrs.validators.instance_of(str)),
    )


@attrs.define(**attrs_defaults.ATTRS_DEFAULTS)
class TableSample(attrs_defaults.HasFromDict):  # pylint: disable=too-few-public-methods
    """
    DTO to include the table reference for the sample
    """

    table_reference: TableReference = attrs.field(
        validator=attrs.validators.instance_of(TableReference)
    )
    sample: Sample = attrs.field(validator=attrs.validators.instance_of(Sample))

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
        sample_size = {
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
            "sort_by": "my_column",
            "sort_direction": "DESC"
        }
    """

    sort_by: str = attrs.field(validator=attrs.validators.instance_of(str))
    sort_direction: str = attrs.field(validator=attrs.validators.instance_of(str))

    @sort_by.validator
    def _is_sort_by_valid(  # pylint: disable=no-self-use
        self, attribute: attrs.Attribute, value: Any
    ) -> None:
        if not value or value.strip() != value:
            raise ValueError(
                f'Attribute <{attribute.name}> must be a non-empty stripped string, got: <{value}>'
            )

    @sort_direction.validator
    def _is_sort_direction_valid(  # pylint: disable=no-self-use
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
    RELATIONAL = "relational"

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
            "type": "relational",
            "properties": {
                "sort_by": "my_column",
                "sort_direction": "DESC"
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
class Sample(attrs_defaults.HasFromDict):  # pylint: disable=too-few-public-methods
    """
    DTO for a sample definition as in::
        sample = {
            "sample_size": {
                "count": 123,
                "percentage": 19.2
            },
            "sort_algorithm": {
                "type": "relational",
                "properties": {
                    "sort_by": "my_column",
                    "sort_direction": "DESC"
                }
            }
        }
    """

    sample_size: SampleSize = attrs.field(
        default=None,
        validator=attrs.validators.optional(validator=attrs.validators.instance_of(SampleSize)),
    )
    sort_algorithm: SortAlgorithm = attrs.field(
        default=None,
        validator=attrs.validators.optional(validator=attrs.validators.instance_of(SortAlgorithm)),
    )

    def return_value_if_empty(self, value: Any) -> Any:
        """
        Merge strategy instead of empty.
        :param value:
        :return:
        """
        sample_size = self.sample_size
        sort_algorithm = self.sort_algorithm
        if isinstance(value, Sample):
            if sample_size is None:
                sample_size = value.sample_size
            if sort_algorithm is None:
                sort_algorithm = value.sort_algorithm
        return Sample(sample_size=sample_size, sort_algorithm=sort_algorithm)


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

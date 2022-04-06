# vim: ai:sw=4:ts=4:sta:et:fo=croql
"""
DTOs to encode the sample request.
"""
import enum
import logging
from typing import Any, Dict

import attrs

from bq_sampler.dto import attrs_defaults


@attrs.define(**attrs_defaults.ATTRS_DEFAULTS)
class SampleSize:  # pylint: disable=too-few-public-methods
    """
    Sample size as in::
        sample_size = {
            "count": 123,
            "percentage": 10.1,
        }
    """

    count: int = attrs.field(validator=attrs.validators.optional(validator=attrs.validators.gt(0)))
    percentage: float = attrs.field(
        validator=attrs.validators.optional(
            validator=[attrs.validators.gt(0.0), attrs.validators.le(100.0)]
        )
    )

    @classmethod
    def from_dict(cls, value: Dict[str, Any]) -> Any:
        """
        Converts a simple :py:class:`dict` into a :py:class:`SampleSize`.

        :param value:
        :return:
        """
        result = None
        if isinstance(value, dict):
            try:
                result = cls(count=value.get('count'), percentage=value.get('percentage'))
            except Exception as err:  # pylint: disable=broad-except
                logging.warning(
                    'Could not parse %s from dictionary <%s>. Error: %s', cls.__name__, value, err
                )
        return result


class _EnumWithFromStrIgnoreCase(enum.Enum):
    @classmethod
    def from_str(cls, value: str) -> Any:
        """
        Parses a string value into corresponding :py:class:`enum.Enum`
        comparing it with the values and ignoring case.

        :param value:
        :return:
        """
        result = None
        if value is not None:
            for val in cls:
                if val.value.lower() == value.lower().strip():
                    result = val
                    break
        return result


class SortDirection(_EnumWithFromStrIgnoreCase):
    """
    Available sorting directions.
    """

    ASC = "ASC"
    DESC = "DESC"


@attrs.define(**attrs_defaults.ATTRS_DEFAULTS)
class _SortProperties:  # pylint: disable=too-few-public-methods
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
    def _is_sort_by_valid(
        self, attribute: attrs.Attribute, value: Any
    ) -> None:  # pylint: disable=no-self-use
        if not value or value.strip() != value:
            raise ValueError(
                f'Attribute <{attribute.name}> must be a non-empty stripped string, got: <{value}>'
            )

    @sort_direction.validator
    def _is_sort_direction_valid(
        self, attribute: attrs.Attribute, value: Any
    ) -> None:  # pylint: disable=no-self-use
        if not SortDirection.from_str(value):
            raise ValueError(
                f'Attribute <{attribute.name}> must be one of {[d.value for d in SortDirection]},'
                f' got: <{value}>'
            )

    @classmethod
    def from_dict(cls, value: Dict[str, Any]) -> Any:
        """
        Converts a simple :py:class:`dict` into a :py:class:`Policy`.

        :param value:
        :return:
        """
        result = None
        if isinstance(value, dict):
            try:
                result = cls(
                    sort_by=value.get('sort_by'), sort_direction=value.get('sort_direction')
                )
            except Exception as err:  # pylint: disable=broad-except
                logging.warning(
                    'Could not parse %s from dictionary <%s>. Error: %s', cls.__name__, value, err
                )
        return result


class SortType(_EnumWithFromStrIgnoreCase):
    """
    Which type of sorting is supported in the sample.
    """

    RANDOM = "random"
    RELATIONAL = "relational"


@attrs.define(**attrs_defaults.ATTRS_DEFAULTS)
class _SortAlgorithm:  # pylint: disable=too-few-public-methods
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
        validator=attrs.validators.optional(validator=attrs.validators.instance_of(str))
    )
    properties: _SortProperties = attrs.field(
        validator=attrs.validators.optional(validator=attrs.validators.instance_of(_SortProperties))
    )

    @type.validator
    def _is_type_valid(
        self, attribute: attrs.Attribute, value: Any
    ) -> None:  # pylint: disable=no-self-use
        if value is not None and SortType.from_str(value) is None:
            raise ValueError(
                f'Attribute <{attribute.name}> needs to be one of {[t.value for t in SortType]},'
                f' got: <{value}>'
            )

    @classmethod
    def from_dict(cls, value: Dict[str, Any]) -> Any:
        """
        Converts a simple :py:class:`dict` into a :py:class:`Policy`.

        :param value:
        :return:
        """
        result = None
        if isinstance(value, dict):
            try:
                properties = _SortProperties.from_dict(value.get('properties'))
                result = _SortAlgorithm(type=value.get('type'), properties=properties)
            except Exception as err:  # pylint: disable=broad-except
                logging.warning(
                    'Could not parse %s from dictionary <%s>. Error: %s', cls.__name__, value, err
                )
        return result


@attrs.define(**attrs_defaults.ATTRS_DEFAULTS)
class Sample:  # pylint: disable=too-few-public-methods
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
        validator=attrs.validators.optional(validator=attrs.validators.instance_of(SampleSize))
    )
    sort_algorithm: _SortAlgorithm = attrs.field(
        validator=attrs.validators.optional(validator=attrs.validators.instance_of(_SortAlgorithm))
    )

    @classmethod
    def from_dict(cls, value: Dict[str, Any]) -> Any:
        """
        Converts a simple :py:class:`dict` into a :py:class:`Policy`.

        :param value:
        :return:
        """
        result = None
        if isinstance(value, dict):
            sample_size = SampleSize.from_dict(value.get('sample_size'))
            sort_algorithm = _SortAlgorithm.from_dict(value.get('sort_algorithm'))
            result = cls(sample_size=sample_size, sort_algorithm=sort_algorithm)
        return result

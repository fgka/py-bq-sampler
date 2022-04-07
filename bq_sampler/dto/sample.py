# vim: ai:sw=4:ts=4:sta:et:fo=croql
"""
DTOs to encode the sample request.
"""
import enum
import logging
from typing import Any, Dict, Optional

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

    count: int = attrs.field(
        default=None, validator=attrs.validators.optional(validator=attrs.validators.gt(0))
    )
    percentage: float = attrs.field(
        default=None,
        validator=attrs.validators.optional(
            validator=[attrs.validators.gt(0.0), attrs.validators.le(100.0)]
        ),
    )

    def overwrite_with(self, value: Optional[Any]) -> Any:
        """
        Creates a new instance of :py:class:`SampleSize` where:
        - the current object as either `count` or `percentage` is defined;
        - if all is attributes are :py:obj:`None` returns `value`.

        :param value:
        :return:
        """
        result = self
        if self.is_empty() and isinstance(value, SampleSize):
            result = value
        return result

    def is_empty(self) -> bool:
        """
        Returns :py:obj:`True` if all attributes are :py:obj:`None`.
        :return:
        """
        return self.count is None and self.percentage is None

    @classmethod
    def from_dict(cls, value: Dict[str, Any]) -> Any:  # pylint: disable=duplicate-code
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

    @classmethod
    def default(cls) -> Any:
        return SortType.RANDOM


@attrs.define(**attrs_defaults.ATTRS_DEFAULTS)
class SortAlgorithm:  # pylint: disable=too-few-public-methods
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
                result = SortAlgorithm(type=value.get('type'), properties=properties)
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
        default=None,
        validator=attrs.validators.optional(validator=attrs.validators.instance_of(SampleSize)),
    )
    sort_algorithm: SortAlgorithm = attrs.field(
        default=None,
        validator=attrs.validators.optional(validator=attrs.validators.instance_of(SortAlgorithm)),
    )

    def overwrite_with(self, value: Optional[Any]) -> Any:
        """
        Creates a new instance of :py:class:`Sample`
        where its attributes are the result of calling `overwrite_with` on them.

        :param value:
        :return:
        """
        result = self
        if self.is_emtpy() and isinstance(value, Sample):
            sample_size = (
                self.sample_size.overwrite_with(value.sample_size)
                if self.sample_size is not None
                else value.sample_size
            )
            sort_algorithm = (
                self.sort_algorithm
                if self.sort_algorithm is not None
                else value.sort_algorithm
            )
            result = Sample(sample_size=sample_size, sort_algorithm=sort_algorithm)
        return result

    def is_emtpy(self) -> bool:
        """
        Returns :py:obj:`True` if all attributes are :py:obj:`None`.
        :return:
        """
        return self.sample_size is None and self.sort_algorithm is None

    @classmethod
    def from_dict(cls, value: Dict[str, Any]) -> Any:
        """
        Converts a simple :py:class:`dict` into a :py:class:`Policy`.

        :param value:
        :return:
        """
        if isinstance(value, dict):
            sample_size = SampleSize.from_dict(value.get('sample_size'))
            sort_algorithm = SortAlgorithm.from_dict(value.get('sort_algorithm'))
            result = cls(sample_size=sample_size, sort_algorithm=sort_algorithm)
        else:
            result = Sample()
        return result


@attrs.define(**attrs_defaults.ATTRS_DEFAULTS)
class TableReference:  # pylint: disable=too-few-public-methods
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

    @classmethod
    def from_dict(cls, value: Dict[str, Any]) -> Any:
        """
        Converts a simple :py:class:`dict` into a :py:class:`TableReference`.

        :param value:
        :return:
        """
        result = None
        if isinstance(value, dict):
            result = cls(
                project_id=value.get('project_id'),
                dataset_id=value.get('dataset_id'),
                table_id=value.get('table_id'),
                region=value.get('region'),
            )
        return result


@attrs.define(**attrs_defaults.ATTRS_DEFAULTS)
class TableSample:  # pylint: disable=too-few-public-methods
    """
    DTO to include the table reference for the sample
    """

    table_reference: TableReference = attrs.field(
        validator=attrs.validators.instance_of(TableReference)
    )
    sample: Sample = attrs.field(validator=attrs.validators.instance_of(Sample))

    @classmethod
    def from_dict(cls, value: Dict[str, Any]) -> Any:
        """
        Converts a simple :py:class:`dict` into a :py:class:`TableSample`.

        :param value:
        :return:
        """
        result = None
        if isinstance(value, dict):
            table_reference = TableReference.from_dict(value.get('table_reference'))
            sample = Sample.from_dict(value.get('sample'))
            result = cls(table_reference=table_reference, sample=sample)
        return result

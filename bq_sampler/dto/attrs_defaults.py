# vim: ai:sw=4:ts=4:sta:et:fo=croql
"""
Default values for creating an attributes class. To be used as::

    import attrs

    @attrs.define(**attrs_defaults.ATTRS_DEFAULTS)
    class MyAttrs: pass
"""
import enum
import logging
from typing import Any, Dict

import attrs

ATTRS_DEFAULTS: Dict[str, Any] = dict(
    kw_only=True,
    str=True,
    repr=True,
    eq=True,
    hash=True,
    frozen=True,
    slots=True,
)
"""
See `attrs`_ documentation for details.

.. _attrs: https://www.attrs.org/en/stable/api.html#attr.s
"""


class HasIsEmpty:  # pylint: disable=too-few-public-methods
    """
    To add :py:meth:`is_empty` to children.
    """

    def is_empty(self) -> bool:
        """
        Check all fields and returns :py:obj:`True` if they are all :py:obj:`None`.
        :return:
        """
        # pylint: disable=use-a-generator
        return all([val is None for val in attrs.asdict(self).values()])


class HasReturnArgumentIfEmpty(HasIsEmpty):
    """
    To add :py:meth:`return_value_if_empty` to children.
    """

    def return_value_if_empty(self, value: Any) -> Any:
        """
        If the current object is empty **AND** `value` is of the same class, return `value`.
        :param value:
        :return:
        """
        result = self
        if self.is_empty() and isinstance(value, self.__class__):
            result = value
        return result


class HasFromDict(HasReturnArgumentIfEmpty):
    """
    To add :py:meth:`from_dict` to children.
    """

    @classmethod
    def from_dict(cls, value: Dict[str, Any]) -> Any:  # pylint: disable=duplicate-code
        """
        Converts a simple :py:class:`dict` into a instance of the current class.

        :param value:
        :return:
        """
        kwargs = {}
        if isinstance(value, dict):
            try:
                kwargs = cls._create_kwargs(value)
            except Exception as err:  # pylint: disable=broad-except
                logging.warning(
                    'Could not parse %s from dictionary <%s>. Error: %s', cls.__name__, value, err
                )
        try:
            result = cls(**kwargs)
        except Exception as err:  # pylint: disable=broad-except
            msg = f'Could not instantiate <{cls.__name__}> from kwargs <{kwargs}>. Error: {err}'
            logging.critical(msg)
            raise ValueError(msg) from err
        return result

    @classmethod
    def _create_kwargs(cls, value: Dict[str, Any]) -> Dict[str, Any]:
        """
        Because of how :py:module:`attrs` works, the `value` must be trimmed
        to the exact attributes available to be used as `kwargs`.

        **NOTE**: It a field is of type :py:class:`HasFromDict`
                  it will call :py:meth:`from_dict` recursively.

        :param value:
        :return:
        """
        result = {}
        for field in list(attrs.fields(cls)):
            field_value = value.get(field.name)
            if field_value is not None and issubclass(field.type, HasFromDict):
                # recursion on from_dict()
                try:
                    field_value = field.type.from_dict(field_value)
                except Exception as err:  # pylint: disable=broad-except
                    field_value = None
                    logging.warning(
                        'Could create field <%s> from dict. Ignoring. Error: %s', field.name, err
                    )
            result[field.name] = field_value
        return result


class EnumWithFromStrIgnoreCase(enum.Enum):
    """
    To add :py:meth:`from_str` to children.
    """

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

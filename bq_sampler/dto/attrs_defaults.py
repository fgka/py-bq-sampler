# vim: ai:sw=4:ts=4:sta:et:fo=croql
"""
Default values for creating an attributes class. To be used as::

    import attrs

    @attrs.define(**attrs_defaults.ATTRS_DEFAULTS)
    class MyAttrs: pass
"""
import enum
import json
import logging
from typing import Any, Dict

import attrs


_LOGGER = logging.getLogger(__name__)


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


class HasPatchWith(HasIsEmpty):
    """
    To add :py:meth:`return_value_if_empty` to children.
    """

    def patch_is_substitution(self) -> bool:  # pylint: disable=no-self-use
        """
        Controls how :py:meth:`patch_with` works. If it is complete substitution or a merge.

        :return:
        """
        return True

    def patch_with(self, value: Any) -> Any:
        """
        The behavior depends on :py:meth:`patch_is_substitution`.
        The argument `value` is only considered if it is of the same type as current instance.

        If :py:meth:`patch_is_substitution` is :py:obj:`True` will return only return `value`
        if the current instance is empty, i.e., :py:meth:`is_empty` returns :py:obj:`True`.

        If :py:meth:`patch_is_substitution` is :py:obj:`False`,
        will check each attribute individually and apply :py:meth:`patch_with` if applicable.

        **NOTE**: It never changes the involved objects.
        If the merge strategy is chosen, will create a new object with merge result.

        :param value:
        :return:
        """
        result = self
        if self.patch_is_substitution():
            # substitution
            if self.is_empty() and isinstance(value, self.__class__):
                result = value
        else:
            # merge
            if isinstance(value, self.__class__):
                result = self._merge(value)
        return result

    def _merge(self, value: Any) -> Any:
        try:
            kwargs = self._create_merge_kwargs(value)
        except Exception as err:  # pylint: disable=broad-except[
            msg = (
                f'Could not create merge kwargs. Current object: <{self}>. '
                f'Value: <{value}>. '
                f'Error: {err}'
            )
            _LOGGER.critical(msg)
            raise ValueError(msg) from err
        try:
            result = self.__class__(**kwargs)
        except Exception as err:  # pylint: disable=broad-except
            msg = (
                f'Could not instantiate <{self.__class__.__name__}> '
                f'from kwargs <{kwargs}>. '
                f'Error: {err}'
            )
            _LOGGER.critical(msg)
            raise ValueError(msg) from err
        return result

    def _create_merge_kwargs(self, value: Any) -> Dict[str, Any]:
        result = {}
        for field in list(attrs.fields(self.__class__)):
            self_field = getattr(self, field.name)
            value_field = getattr(value, field.name)
            result_field = self_field
            if self_field is None:
                # clear substitution
                result_field = value_field
            elif (
                self_field is not None
                and value_field is not None
                and issubclass(field.type, HasPatchWith)
            ):
                # recursion on patch_with()
                try:
                    result_field = self_field.patch_with(value_field)
                except Exception as err:  # pylint: disable=broad-except
                    value_field = None
                    _LOGGER.warning(
                        'Could create field <%s> patching <%s> with <%s>. Ignoring. Error: %s',
                        field.name,
                        self_field,
                        value_field,
                        err,
                    )
            result[field.name] = result_field
        return result


class HasFromDict(HasPatchWith):
    """
    To add :py:meth:`from_dict` to children.
    """

    @classmethod
    def from_dict(cls, value: Dict[str, Any]) -> Any:
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
                _LOGGER.warning(
                    'Could not parse %s from dictionary <%s>. Error: %s', cls.__name__, value, err
                )
        try:
            result = cls(**kwargs)
        except Exception as err:  # pylint: disable=broad-except
            msg = f'Could not instantiate <{cls.__name__}> from kwargs <{kwargs}>. Error: {err}'
            _LOGGER.critical(msg)
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
                    _LOGGER.warning(
                        'Could create field <%s> from dict. Ignoring. Error: %s', field.name, err
                    )
            result[field.name] = field_value
        return result


class HasFromJsonString(HasFromDict):
    """
    To add :py:meth:`from_json` to children.
    """

    @classmethod
    def from_json(cls, json_string: str) -> Any:
        """
        Will parse `json_string` and use :py:meth:`from_dict` to get the instance.

        :param json_string:
        :return:
        """
        value = {}
        try:
            value = json.loads(json_string)
        except Exception as err:  # pylint: disable=broad-except
            _LOGGER.warning(
                'Could not parse JSON string <%s>. Ignoring. Error: %s', json_string, err
            )
        return cls.from_dict(value)

    def as_json(self) -> str:
        """
        Converts the current object into a JSON string.

        :return:
        """
        # first to dict
        try:
            value_dict = attrs.asdict(self)
        except Exception as err:  # pylint: disable=broad-except
            msg = f'Could not convert <{self}> to a dictionary. Error: {err}'
            _LOGGER.critical(msg)
            raise ValueError(msg) from err
        # now to a JSON string from the dict
        try:
            result = json.dumps(value_dict)
        except Exception as err:  # pylint: disable=broad-except
            msg = f'Could not convert <{value_dict}>, from <{self}>, to a JSON string. Error: {err}'
            _LOGGER.critical(msg)
            raise ValueError(msg) from err
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

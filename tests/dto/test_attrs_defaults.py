# vim: ai:sw=4:ts=4:sta:et:fo=croql
# pylint: disable=missing-module-docstring,missing-class-docstring,missing-function-docstring
# pylint: disable=missing-function-docstring,assignment-from-no-return,c-extension-no-member
# pylint: disable=protected-access,redefined-outer-name,no-self-use,using-constant-test
# pylint: disable=invalid-name,attribute-defined-outside-init,too-few-public-methods, redefined-builtin
# type: ignore

from typing import List

import pytest

import attrs

from bq_sampler.dto import attrs_defaults


@attrs.define(**attrs_defaults.ATTRS_DEFAULTS)
class _MyHasIsEmpty(attrs_defaults.HasIsEmpty):
    field_int: int = attrs.field(default=None)
    field_str: str = attrs.field(default=None)


class TestHasIsEmpty:
    def test_is_empty_ok_true_default_ctor(self):
        # Given
        obj = _MyHasIsEmpty()
        # When/Then
        assert obj.is_empty()

    def test_is_empty_ok_true_ctor(self):
        # Given
        obj = _MyHasIsEmpty(field_int=None, field_str=None)
        # When/Then
        assert obj.is_empty()

    @pytest.mark.parametrize(
        'field_int,field_str',
        [
            (0, None),
            (None, ''),
            (0, ''),
        ],
    )
    def test_is_empty_ok_false_ctor(self, field_int: int, field_str: str):
        # Given
        obj = _MyHasIsEmpty(field_int=field_int, field_str=field_str)
        # When/Then
        assert not obj.is_empty()


@attrs.define(**attrs_defaults.ATTRS_DEFAULTS)
class _MyHasReturnArgumentIfEmpty(attrs_defaults.HasReturnArgumentIfEmpty):
    field_int: int = attrs.field(default=None)
    field_str: str = attrs.field(default=None)


_TEST_MY_HAS_RETURN_ARGUMENT_IF_EMPTY = _MyHasReturnArgumentIfEmpty(field_int=11, field_str='TEST')


class TestHasReturnArgumentIfEmpty:
    @pytest.mark.parametrize(
        'field_int,field_str',
        [
            (0, None),
            (None, ''),
            (0, ''),
        ],
    )
    def test_return_value_if_empty_ok_return_self(self, field_int: int, field_str: str):
        # Given
        obj = _MyHasReturnArgumentIfEmpty(field_int=field_int, field_str=field_str)
        # When
        result = obj.return_value_if_empty(_TEST_MY_HAS_RETURN_ARGUMENT_IF_EMPTY)
        # Then
        assert result == obj

    def test_return_value_if_empty_ok_return_self_value_none(self):
        # Given
        obj = _MyHasReturnArgumentIfEmpty()
        # When
        result = obj.return_value_if_empty(None)
        # Then
        assert result == obj

    def test_return_value_if_empty_ok_return_value(self):
        # Given
        obj = _MyHasReturnArgumentIfEmpty()
        # When
        result = obj.return_value_if_empty(_TEST_MY_HAS_RETURN_ARGUMENT_IF_EMPTY)
        # Then
        assert result == _TEST_MY_HAS_RETURN_ARGUMENT_IF_EMPTY


@attrs.define(**attrs_defaults.ATTRS_DEFAULTS)
class _MyHasFromDictA(attrs_defaults.HasFromDict):
    field_int: int = attrs.field(default=None)
    field_str: str = attrs.field(default=None)


_TEST_MY_HAS_FROM_DICT_A = _MyHasFromDictA(field_int=41, field_str='TEST_CASE')


@attrs.define(**attrs_defaults.ATTRS_DEFAULTS)
class _MyHasFromDictB(attrs_defaults.HasFromDict):
    field_int: int = attrs.field(default=None)
    field_a: _MyHasFromDictA = attrs.field(default=None)


class TestHasFromDict:
    @pytest.mark.parametrize(
        'field_int,field_str',
        [
            (None, None),
            (0, None),
            (None, ''),
            (0, ''),
            (31, 'TEST'),
        ],
    )
    def test_from_dict_ok_simple(self, field_int: int, field_str: str):
        # Given
        obj = _MyHasFromDictA(field_int=field_int, field_str=field_str)
        # When
        result = obj.from_dict(attrs.asdict(obj))
        # Then
        assert result == obj

    @pytest.mark.parametrize(
        'field_int,field_a',
        [
            (None, None),
            (0, None),
            (None, _TEST_MY_HAS_FROM_DICT_A),
            (0, _TEST_MY_HAS_FROM_DICT_A),
            (31, _TEST_MY_HAS_FROM_DICT_A),
        ],
    )
    def test_from_dict_ok_recursive(self, field_int: int, field_a: _MyHasFromDictA):
        # Given
        obj = _MyHasFromDictB(field_int=field_int, field_a=field_a)
        # When
        result = obj.from_dict(attrs.asdict(obj))
        # Then
        assert result == obj
        if field_a is not None:
            assert result.field_a == field_a


class _MyEnumWithFromStrIgnoreCase(attrs_defaults.EnumWithFromStrIgnoreCase):
    ITEM_A = 'Item_A'
    ITEM_B = 'ITEM_B'
    ITEM_C = 'item_C'


class TestEnumWithFromStrIgnoreCase:
    def test_from_str_ok(self):
        # Given
        for item in _MyEnumWithFromStrIgnoreCase:
            for cased_item in self._multiple_case_variations(item.value):
                # When
                result = _MyEnumWithFromStrIgnoreCase.from_str(cased_item)
                # Then
                assert result == item

    @staticmethod
    def _multiple_case_variations(value: str) -> List[str]:
        return [value.lower(), value.upper(), value.capitalize()]

    def test_from_str_nok_not_matching(self):
        # Given
        for item in _MyEnumWithFromStrIgnoreCase:
            # When
            result = _MyEnumWithFromStrIgnoreCase.from_str(item.value + '_NOT')
            # Then
            assert result is None

    def test_from_str_nok_none(self):
        # Given/When
        result = _MyEnumWithFromStrIgnoreCase.from_str(None)
        # Then
        assert result is None

# vim: ai:sw=4:ts=4:sta:et:fo=croql
# pylint: disable=missing-module-docstring,missing-class-docstring,missing-function-docstring
# pylint: disable=missing-function-docstring,assignment-from-no-return,c-extension-no-member
# pylint: disable=protected-access,redefined-outer-name,no-self-use,using-constant-test
# pylint: disable=invalid-name,attribute-defined-outside-init,too-few-public-methods, redefined-builtin
# type: ignore

from typing import Any, Dict, List, Optional

import pytest

import attrs
import deepdiff

from bq_sampler import const
from bq_sampler.entity import attrs_defaults


@attrs.define(**const.ATTRS_DEFAULTS)
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


@attrs.define(**const.ATTRS_DEFAULTS)
class _MyHasPatchWith_Substitution(attrs_defaults.HasPatchWith):
    field_int: int = attrs.field(default=None)
    field_str: str = attrs.field(default=None)

    def patch_is_substitution(self) -> bool:
        return True


_TEST_MY_HAS_PATCH_WITH_SUBSTITUTION = _MyHasPatchWith_Substitution(
    field_int=11, field_str='TEST_SUBSTITUTION'
)


class TestHasPatchWith_Substitution:
    @pytest.mark.parametrize(
        'field_int,field_str',
        [
            (0, None),
            (None, ''),
            (0, ''),
        ],
    )
    def test_patch_with_ok_return_self(self, field_int: int, field_str: str):
        # Given
        obj = _MyHasPatchWith_Substitution(field_int=field_int, field_str=field_str)
        # When
        result = obj.patch_with(_TEST_MY_HAS_PATCH_WITH_SUBSTITUTION)
        # Then
        assert result == obj

    def test_patch_with_ok_return_self_value_none(self):
        # Given
        obj = _MyHasPatchWith_Substitution()
        # When
        result = obj.patch_with(None)
        # Then
        assert result == obj

    def test_patch_with_ok_return_value(self):
        # Given
        obj = _MyHasPatchWith_Substitution()
        # When
        result = obj.patch_with(_TEST_MY_HAS_PATCH_WITH_SUBSTITUTION)
        # Then
        assert result == _TEST_MY_HAS_PATCH_WITH_SUBSTITUTION


@attrs.define(**const.ATTRS_DEFAULTS)
class _MyHasPatchWith_Merge(attrs_defaults.HasPatchWith):
    field_int: int = attrs.field(default=None)
    field_with_patch: attrs_defaults.HasPatchWith = attrs.field(default=None)

    def patch_is_substitution(self) -> bool:
        return False


_TEST_MY_HAS_PATCH_WITH_MERGE_A = _MyHasPatchWith_Merge(
    field_int=13, field_with_patch=_TEST_MY_HAS_PATCH_WITH_SUBSTITUTION
)
_TEST_MY_HAS_PATCH_WITH_MERGE_B = _MyHasPatchWith_Merge(
    field_int=17, field_with_patch=_TEST_MY_HAS_PATCH_WITH_MERGE_A
)


class TestHasPatchWith_Merge:
    def test_patch_with_ok_return_self(self):
        # Given
        obj = _MyHasPatchWith_Merge(
            field_int=23, field_with_patch=_TEST_MY_HAS_PATCH_WITH_SUBSTITUTION
        )
        # When
        result = obj.patch_with(_TEST_MY_HAS_PATCH_WITH_MERGE_B)
        # Then
        assert result == obj

    def test_patch_with_ok_return_self_value_none(self):
        # Given
        obj = _MyHasPatchWith_Merge()
        # When
        result = obj.patch_with(None)
        # Then
        assert result == obj

    def test_patch_with_ok_return_value(self):
        # Given
        obj = _MyHasPatchWith_Merge()
        # When
        result = obj.patch_with(_TEST_MY_HAS_PATCH_WITH_MERGE_B)
        # Then
        assert result == _TEST_MY_HAS_PATCH_WITH_MERGE_B

    def test_patch_with_ok_return_value_field_int(self):
        # Given
        obj = _MyHasPatchWith_Merge(field_with_patch=_TEST_MY_HAS_PATCH_WITH_SUBSTITUTION)
        # When
        result = obj.patch_with(_TEST_MY_HAS_PATCH_WITH_MERGE_B)
        # Then
        assert result.field_int == _TEST_MY_HAS_PATCH_WITH_MERGE_B.field_int
        assert result.field_with_patch == obj.field_with_patch

    def test_patch_with_ok_return_value_field_with_patch(self):
        # Given
        obj = _MyHasPatchWith_Merge(field_int=31)
        # When
        result = obj.patch_with(_TEST_MY_HAS_PATCH_WITH_MERGE_B)
        # Then
        assert result.field_int == obj.field_int
        assert result.field_with_patch == _TEST_MY_HAS_PATCH_WITH_MERGE_B.field_with_patch

    def test_patch_with_ok_recursion(self):
        # Given
        obj = _MyHasPatchWith_Merge(
            field_with_patch=_MyHasPatchWith_Merge(
                field_with_patch=_TEST_MY_HAS_PATCH_WITH_SUBSTITUTION
            )
        )
        value = _MyHasPatchWith_Merge(
            field_int=29, field_with_patch=_MyHasPatchWith_Merge(field_int=31)
        )
        # When
        result = obj.patch_with(value)
        # Then
        assert result.field_int == value.field_int
        # Then: recursion
        assert result.field_with_patch.field_int == value.field_with_patch.field_int
        assert result.field_with_patch.field_with_patch == obj.field_with_patch.field_with_patch


@attrs.define(**const.ATTRS_DEFAULTS)
class _MyHasFromDictA(attrs_defaults.HasFromDict):
    field_int: int = attrs.field(default=None)
    field_str: str = attrs.field(default=None)


_TEST_MY_HAS_FROM_DICT_A = _MyHasFromDictA(field_int=41, field_str='TEST_CASE')


@attrs.define(**const.ATTRS_DEFAULTS)
class _MyHasFromDictB(attrs_defaults.HasFromDict):
    field_int: int = attrs.field(default=None)
    field_a: _MyHasFromDictA = attrs.field(default=None)


class TestHasFromDict:
    def test_as_dict_ok(self):
        # Given
        obj = _MyHasFromDictA(field_int=17, field_str='test')
        # When
        result = obj.as_dict()
        # Then
        diff = deepdiff.DeepDiff(result, attrs.asdict(obj))
        assert not diff, diff

    @pytest.mark.parametrize('overwrite', [{}, {'not_a_field': 19}])
    def test_clone_ok_no_overwrite(self, overwrite: Optional[Dict[str, Any]]):
        # Given
        obj = _MyHasFromDictA(field_int=17, field_str='test')
        # When
        result = obj.clone(**overwrite)
        # Then
        assert obj == result

    def test_clone_ok_with_overwrite(self):
        # Given
        overwrite = {'field_int': 31}
        obj = _MyHasFromDictA(field_int=17, field_str='test')
        # When
        result = obj.clone(**overwrite)
        # Then
        assert obj != result
        assert obj.field_str == result.field_str
        assert result.field_int == overwrite.get('field_int')

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


@attrs.define(**const.ATTRS_DEFAULTS)
class _MyHasFromJsonStringA(attrs_defaults.HasFromJsonString):
    field_str: str = attrs.field(default=None)
    field_float: float = attrs.field(default=None)


@attrs.define(**const.ATTRS_DEFAULTS)
class _MyHasFromJsonStringB(attrs_defaults.HasFromJsonString):
    field_int: int = attrs.field(default=None)
    field_a: _MyHasFromJsonStringA = attrs.field(default=None)


class TestHasFromJsonString:
    def test_from_json_ok_recursion(self):
        # Given
        obj = _MyHasFromJsonStringB(
            field_int=17, field_a=_MyHasFromJsonStringA(field_str="TEST_JSON", field_float="1.1")
        )
        # When
        result = _MyHasFromJsonStringB.from_json(obj.as_json())
        # Then
        assert result == obj

    @pytest.mark.parametrize(
        'json_string',
        [
            None,
            "",
            "{}",
            "{field_int:17}",  # Field is missing quotation
        ],
    )
    def test_from_json_ok_empty_object(self, json_string: Any):
        # Given/When
        obj = _MyHasFromJsonStringB.from_json(json_string)
        # Then
        assert isinstance(obj, _MyHasFromJsonStringB)
        assert obj.is_empty()


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

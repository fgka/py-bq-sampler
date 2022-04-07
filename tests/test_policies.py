# vim: ai:sw=4:ts=4:sta:et:fo=croql
# pylint: disable=missing-module-docstring,missing-class-docstring,missing-function-docstring
# pylint: disable=missing-function-docstring,assignment-from-no-return,c-extension-no-member
# pylint: disable=protected-access,redefined-outer-name,no-self-use,using-constant-test
# pylint: disable=invalid-name,attribute-defined-outside-init,too-few-public-methods, redefined-builtin
# type: ignore

from typing import Any

import pytest

from bq_sampler import gcs_bucket


def test_get_all_policies():
    result = gcs_bucket.get_all_policies(
        bucket_name='test_bucket_gka', generic_policy_filename='sample_policy.json'
    )
    print(result)


def test_get_all_sample_requests():
    result = gcs_bucket.get_all_sample_requests(bucket_name='test_bucket_gka')
    print(result)

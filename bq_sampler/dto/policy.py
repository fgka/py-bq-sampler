# vim: ai:sw=4:ts=4:sta:et:fo=croql
"""
DTO for the sample policy to be used to validate given sample request.
"""
from typing import Any, Dict, Optional

import attrs

from bq_sampler.dto import attrs_defaults
from bq_sampler.dto import sample


@attrs.define(**attrs_defaults.ATTRS_DEFAULTS)
class Policy:  # pylint: disable=too-few-public-methods
    """
    Holds the DTO corresponding to a sample policy, as in::
        policy = {
            "sample_size_limit": {
                "count": 1000,
                "percentage": 20.0
            },
            "default_sample": {
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
        }
    """

    sample_size_limit: sample.SampleSize = attrs.field(
        default=None,
        validator=attrs.validators.optional(
            validator=attrs.validators.instance_of(sample.SampleSize)
        ),
    )
    default_sample: sample.Sample = attrs.field(
        default=None,
        validator=attrs.validators.optional(validator=attrs.validators.instance_of(sample.Sample)),
    )

    def overwrite_with(self, value: Optional[Any]) -> Any:
        """
        Creates a new instance of :py:class:`Policy` where its attributes
        are the result of calling `overwrite_with` on them.

        :param value:
        :return:
        """
        result = self
        if isinstance(value, Policy):
            sample_size_limit = (
                self.sample_size_limit.overwrite_with(value.sample_size_limit)
                if self.sample_size_limit is not None
                else value.sample_size_limit
            )
            default_sample = (
                self.default_sample.overwrite_with(value.default_sample)
                if self.default_sample is not None
                else value.default_sample
            )
            result = Policy(sample_size_limit=sample_size_limit, default_sample=default_sample)
        return result

    @classmethod
    def from_dict(cls, value: Dict[str, Any]) -> Any:  # pylint: disable=duplicate-code
        """
        Converts a simple :py:class:`dict` into a :py:class:`Policy`.

        :param value:
        :return:
        """
        if isinstance(value, dict):
            sample_size_limit = sample.SampleSize.from_dict(value.get('sample_size_limit'))
            default_sample = sample.Sample.from_dict(value.get('default_sample'))
            result = cls(sample_size_limit=sample_size_limit, default_sample=default_sample)
        else:
            result = Policy()
        return result


FALLBACK_GENERIC_POLICY: Policy = Policy(
    sample_size_limit=sample.SampleSize(count=1),
    default_sample=sample.Sample(
        sample_size=sample.SampleSize(count=1),
        sort_algorithm=sample.SortAlgorithm(type=sample.SortType.RANDOM.value),
    ),
)
"""
To be used as the absolute minimum default policy.
"""


@attrs.define(**attrs_defaults.ATTRS_DEFAULTS)
class TablePolicy:  # pylint: disable=too-few-public-methods
    """
    DTO to include the table reference for the policy
    """

    table_reference: sample.TableReference = attrs.field(
        validator=attrs.validators.instance_of(sample.TableReference)
    )
    policy: Policy = attrs.field(validator=attrs.validators.instance_of(Policy))

    @classmethod
    def from_dict(cls, value: Dict[str, Any]) -> Any:
        """
        Converts a simple :py:class:`dict` into a :py:class:`TablePolicy`.

        :param value:
        :return:
        """
        result = None
        if isinstance(value, dict):
            table_reference = sample.TableReference.from_dict(value.get('table_reference'))
            policy = Policy.from_dict(value.get('policy'))
            result = cls(table_reference=table_reference, policy=policy)
        return result

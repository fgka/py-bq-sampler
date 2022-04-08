# vim: ai:sw=4:ts=4:sta:et:fo=croql
"""
DTO for the sample policy to be used to validate given sample request.
"""
from typing import Any

import attrs

from bq_sampler.dto import attrs_defaults
from bq_sampler.dto import sample


@attrs.define(**attrs_defaults.ATTRS_DEFAULTS)
class Policy(attrs_defaults.HasFromDict):  # pylint: disable=too-few-public-methods
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

    def return_value_if_empty(self, value: Any) -> Any:
        """
        Merge strategy instead of empty.
        :param value:
        :return:
        """
        sample_size_limit = self.sample_size_limit
        default_sample = self.default_sample
        if isinstance(value, Policy):
            if sample_size_limit is None:
                sample_size_limit = value.sample_size_limit
            if default_sample is None:
                default_sample = value.default_sample
        return Policy(sample_size_limit=sample_size_limit, default_sample=default_sample)


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
class TablePolicy(attrs_defaults.HasFromDict):  # pylint: disable=too-few-public-methods
    """
    DTO to include the table reference for the policy
    """

    table_reference: sample.TableReference = attrs.field(
        validator=attrs.validators.instance_of(sample.TableReference)
    )
    policy: Policy = attrs.field(validator=attrs.validators.instance_of(Policy))

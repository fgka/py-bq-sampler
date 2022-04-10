# vim: ai:sw=4:ts=4:sta:et:fo=croql
"""
DTO for the sample policy to be used to validate given sample request.
"""

import attrs

from bq_sampler.dto import attrs_defaults
from bq_sampler.dto import sample


@attrs.define(**attrs_defaults.ATTRS_DEFAULTS)
class Policy(attrs_defaults.HasFromJsonString):  # pylint: disable=too-few-public-methods
    """
    Holds the DTO corresponding to a sample policy, as in::
        policy = {
            "limit": {
                "count": 1000,
                "percentage": 20.0
            },
            "default_sample": {
                "size": {
                    "count": 123,
                    "percentage": 19.2
                },
                "spec": {
                    "type": "sorted",
                    "properties": {
                        "by": "my_column",
                        "direction": "DESC"
                    }
                }
            }
        }
    """

    limit: sample.SampleSize = attrs.field(
        default=None,
        validator=attrs.validators.optional(
            validator=attrs.validators.instance_of(sample.SampleSize)
        ),
    )
    default_sample: sample.Sample = attrs.field(
        default=None,
        validator=attrs.validators.optional(validator=attrs.validators.instance_of(sample.Sample)),
    )

    def patch_is_substitution(self) -> bool:
        """
        The rationale for using merge strategy is because a :py:class:`Policy`
        will only be patched against the default policy.

        Example::
            default_policy = {
                "limit": {
                    "count": 1000,
                    "percentage": 20.0
                },
                "default_sample": {
                    "size": {
                        "count": 10,
                        "percentage": 10.0
                    },
                    "spec": {
                        "type": "sorted",
                        "properties": {
                            "by": "my_column",
                            "direction": "DESC"
                        }
                    }
                }
            }
            specific_policy = {
                "default_sample": {
                    "size": {
                        "count": 123,
                        "percentage": 19.2
                    }
                }
            }
            # The policy against which the request will be patched
            # uses default's `limit` from default and also `default_sample.spec`.
            # The specific policy only defines the `default_sample.size`.
            patched_specific_policy = {
                "limit": {
                    "count": 1000,
                    "percentage": 20.0
                },
                "default_sample": {
                    "size": {
                        "count": 123,
                        "percentage": 19.2
                    },
                    "spec": {
                        "type": "sorted",
                        "properties": {
                            "by": "my_column",
                            "direction": "DESC"
                        }
                    }
                }
            }

        :return:
        """
        return False


FALLBACK_GENERIC_POLICY: Policy = Policy(
    limit=sample.SampleSize(count=1),
    default_sample=sample.Sample(
        size=sample.SampleSize(count=1),
        spec=sample.SortAlgorithm(type=sample.SortType.default().value),
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

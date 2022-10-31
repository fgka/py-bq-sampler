# vim: ai:sw=4:ts=4:sta:et:fo=croql
"""
DTO for the sample policy to be used to validate given sample request.
"""
import math

import attrs

from bq_sampler import const
from bq_sampler.entity import attrs_defaults, table


@attrs.define(**const.ATTRS_DEFAULTS)
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

    limit: table.SizeSpec = attrs.field(
        default=None,
        validator=attrs.validators.optional(validator=attrs.validators.instance_of(table.SizeSpec)),
    )
    default_sample: table.Sample = attrs.field(
        default=None,
        validator=attrs.validators.optional(validator=attrs.validators.instance_of(table.Sample)),
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

    def compliant_sample(self, sample: table.Sample, row_count: int) -> table.Sample:
        """
        Will apply the policy to the sample and return a compliant instance.

        :param sample:
        :param row_count:
        :return:
        """
        # validate input
        if not isinstance(sample, table.Sample):
            raise TypeError(
                f'Sample must be of type {table.Sample.__name__}.'
                f' Got <{sample}>({type(sample)})'
            )
        if not isinstance(row_count, (int, float)) or row_count < 0:
            raise ValueError(
                f'Row count must be an integer greater than 0. Got <{row_count}>({type(row_count)})'
            )
        # logic
        policy_count_limit = self._policy_count_limit(row_count)
        sample_count = self._sample_count(sample, row_count)
        compliant_count = min(policy_count_limit, sample_count)
        return self._sample_copy_with_count(sample, compliant_count)

    def _policy_count_limit(self, row_count: int) -> int:
        # row_count works because of min() in percentage
        # and count or percentage must exist at all times
        result = row_count
        if self.limit.count is not None:
            result = self.limit.count
        if self.limit.percentage is not None:
            limit_percent = math.floor(row_count * self.limit.percentage / 100)
            result = min(limit_percent, result)
        return result

    @staticmethod
    def _sample_count(sample: table.Sample, row_count: int) -> int:
        # 0 works because of max() at percentage
        # and count or percentage must exist at all times
        result = 0
        if sample.size.count is not None:
            result = sample.size.count
        if sample.size.percentage is not None:
            limit_percent = math.ceil(row_count * sample.size.percentage / 100)
            result = max(limit_percent, result)
        return result

    @staticmethod
    def _sample_copy_with_count(sample: table.Sample, sample_size: int) -> table.Sample:
        return table.Sample(
            spec=sample.spec,
            size=table.SizeSpec(count=sample_size),
        )


FALLBACK_GENERIC_POLICY: Policy = Policy(
    limit=table.SizeSpec(count=1),
    default_sample=table.Sample(
        size=table.SizeSpec(count=1),
        spec=table.SampleSpec(type=table.SortType.default().value),
    ),
)
"""
To be used as the absolute minimum default policy.
"""


@attrs.define(**const.ATTRS_DEFAULTS)
class TablePolicy(attrs_defaults.HasFromJsonString):  # pylint: disable=too-few-public-methods
    """
    DTO to include the table reference for the policy
    """

    table_reference: table.TableReference = attrs.field(
        validator=attrs.validators.instance_of(table.TableReference)
    )
    policy: Policy = attrs.field(validator=attrs.validators.instance_of(Policy))

    def compliant_sample(
        self, table_sample: table.TableSample, row_count: int
    ) -> table.TableSample:
        """
        Will apply the policy to the table sample and return a compliant instance.

        :param table_sample:
        :param row_count:
        :return:
        """
        # validate input
        if not isinstance(table_sample, table.TableSample):
            raise TypeError(
                f'Table sample must be of type {table.TableSample.__name__}.'
                f' Got <{table_sample}>({type(table_sample)})'
            )
        if table_sample.table_reference != self.table_reference:
            raise ValueError(
                f'Policy only applicable to table {self.table_reference}'
                f' and sample is for table {table_sample.table_reference}'
            )
        # logic
        compliant_sample = self.policy.compliant_sample(table_sample.sample, row_count)
        return table.TableSample(
            table_reference=table_sample.table_reference,
            sample=compliant_sample,
        )

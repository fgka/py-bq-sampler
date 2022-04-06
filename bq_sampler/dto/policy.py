# vim: ai:sw=4:ts=4:sta:et:fo=croql
"""
DTO for the sample policy to be used to validate given sample request.
"""
from typing import Any, Dict

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
            "sample_size_default": {
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
        validator=attrs.validators.optional(
            validator=attrs.validators.instance_of(sample.SampleSize)
        )
    )
    sample_size_default: sample.Sample = attrs.field(
        validator=attrs.validators.optional(validator=attrs.validators.instance_of(sample.Sample))
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
            sample_size_limit = sample.SampleSize.from_dict(value.get('sample_size_limit'))
            sample_size_default = sample.Sample.from_dict(value.get('sample_size_default'))
            result = cls(
                sample_size_limit=sample_size_limit, sample_size_default=sample_size_default
            )
        return result

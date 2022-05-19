# vim: ai:sw=4:ts=4:sta:et:fo=croql
# pylint: disable=line-too-long
"""
Utility to locally test policies and sample requests.
"""
# pylint: enable=line-too-long
import io
import pprint
from typing import Tuple

import attrs
import click

from bq_sampler.entity import policy as policy_
from bq_sampler.entity import table
from bq_sampler import sampler_bucket


@click.group(help='Use this to test your policies and requests.')
def cli() -> None:
    """
    Click entry-point
    :return:
    """


@cli.command(
    help='Outputs the resulting end policy based on the default and specific. '
    'If multiple policies are given, compute each and print them out.'
)
@click.option('--default', '-d', required=True, type=click.File('r'), help='Path to default policy')
@click.option(
    '--policy',
    '-p',
    required=True,
    type=click.File('r'),
    multiple=True,
    help='Path to target/specific policy/policies '
    'You can chain multiple policies by just adding "--policy <FILENAME>"',
)
def policy(  # pylint: disable=redefined-outer-name
    default: io.TextIOWrapper, policy: Tuple[io.TextIOWrapper]
) -> None:
    """
    Debugging for policy files.

    :param default:
    :param policy:
    :return:
    """
    default_policy = _read_and_print_default_policy(default)
    _print_display_separator()
    for pol_text in policy:
        pol = _read_and_print_specific_policy(pol_text)
        _patch_and_print_policy(pol, default_policy)
    _print_display_separator()


def _read_and_print_default_policy(in_text: io.TextIOWrapper) -> policy_.Policy:
    return _read_and_print_policy('Default', in_text)


def _read_and_print_policy(name: str, in_text: io.TextIOWrapper) -> policy_.Policy:
    _print_display_separator()
    result = policy_.Policy.from_json(in_text.read())
    _print_policy(f'{name} policy: {in_text.name}', result)
    return result


def _print_display_separator() -> None:
    print('#' * 20)


def _print_policy(title: str, value: policy_.Policy) -> None:
    pprint.pprint(f'{title}')
    pprint.pprint(attrs.asdict(value))


def _read_and_print_specific_policy(in_text: io.TextIOWrapper) -> policy_.Policy:
    return _read_and_print_policy('Specific', in_text)


def _patch_and_print_policy(policy_a: policy_.Policy, policy_b: policy_.Policy) -> policy_.Policy:
    # pylint: disable=protected-access
    result = sampler_bucket._overwrite_policy(policy_a, policy_b)
    _print_policy('Effective policy:', result)
    return result


@cli.command(
    help='Compute the end policy first based on default and target '
    'and validate the request against. '
    'It prints out the actual request after policy compliance.'
)
@click.option(
    '--default', '-d', required=False, type=click.File('r'), help='Path to default policy'
)
@click.option(
    '--policy',
    '-p',
    required=True,
    type=click.File('r'),
    help='Path to target/specific policy/policies.',
)
@click.option(
    '--size',
    '-s',
    required=True,
    type=int,
    help='Table size against which the compliance of the policy will be checked. '
    'Must be greater than 0.',
)
@click.option('--request', '-r', required=True, type=click.File('r'), help='Path to target request')
def sample_request(  # pylint: disable=redefined-outer-name
    default: io.TextIOWrapper, policy: io.TextIOWrapper, size: int, request: io.TextIOWrapper
) -> None:
    """
    Debugging for sample requests.

    :return:
    """
    default_policy = _read_and_print_default_policy(default)
    pol = _read_and_print_specific_policy(policy)
    effective_policy = _patch_and_print_policy(pol, default_policy)
    _print_display_separator()
    sample_req = _read_and_print_sample_request(request)
    _patch_and_print_request(sample_req, effective_policy, size)
    _print_display_separator()


def _read_and_print_sample_request(in_text: io.TextIOWrapper) -> table.Sample:
    _print_display_separator()
    result = table.Sample.from_json(in_text.read())
    _print_policy(f'Sample request: {in_text.name}', result)
    return result


def _patch_and_print_request(
    request: table.Sample, request_policy: policy_.Policy, size: int
) -> table.Sample:
    # pylint: disable=protected-access
    effective_request = sampler_bucket._overwrite_request(request, request_policy)
    _print_policy('Effective request:', effective_request)
    result = request_policy.compliant_sample(effective_request, size)
    _print_policy('Effective sample:', result)
    return result


if __name__ == '__main__':
    cli()

# vim: ai:sw=4:ts=4:sta:et:fo=croql
# pylint: disable=line-too-long
"""
Utility to locally test policies and sample requests.
"""
# pylint: enable=line-too-long
import io
import os
import pathlib
import pprint
from typing import Generator, Tuple, Union

import attrs
import click

from bq_sampler.entity import policy as policy_
from bq_sampler.entity import table
from bq_sampler import sampler_bucket, process_request


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
    required=False,
    type=click.File('r'),
    multiple=True,
    help='Path to target/specific policy/policies '
    'You can chain multiple policies by just adding "--policy <FILENAME>"',
)
@click.option(
    '--policy-dir',
    required=False,
    type=click.Path(exists=True, readable=True, dir_okay=True, file_okay=False),
    multiple=False,
    help='Path to target/specific policies\' directory '
    'It can NOT be augmented with "--policy <FILENAME>"',
)
@click.option(
    '--request-dir',
    required=False,
    type=click.Path(exists=True, readable=True, dir_okay=True, file_okay=False),
    multiple=False,
    help='Path to requests\' directory '
    'It will also check the corresponding sample against the existing policy',
)
def policy(  # pylint: disable=redefined-outer-name
    default: io.TextIOWrapper, policy: Tuple[io.TextIOWrapper], policy_dir: str, request_dir: str
) -> None:
    """
    Debugging for policy files.

    :param default:
    :param policy:
    :param policy_dir:
    :param request_dir:
    :return:
    """
    default_policy = _read_and_print_default_policy(default)
    _print_display_separator()
    if (policy is None and policy_dir is None) or (policy and policy_dir):
        raise ValueError(
            'You need to specify either a specific policy file(s) or a policy directory. '
            'But not both. '
            f'Got policy: <{policy}>({type(policy)}) '
            f'and policy_dir: {policy_dir}({type(policy_dir)})'
        )
    if policy_dir is None and request_dir:
        print(
            f"Ignoring request dir <{request_dir}>({type(request_dir)} "
            "because it requires --policy-dir argument."
        )
        request_dir = None
    if policy_dir is not None:
        # is a generator
        policy = _list_dir(pathlib.Path(policy_dir))
    _print_display_separator()
    if request_dir:
        for pol, req in _read_request_for_policies(
            pathlib.Path(request_dir), pathlib.Path(policy_dir)
        ):
            actual_pol = _patch_and_print_policy(pol, default_policy)
            actual_req = process_request._compliant_sample_request(actual_pol, req)
            _print_policy_request_pair(actual_pol, actual_req)
    else:
        for pol_text in policy:
            pol = _read_and_print_specific_policy(pol_text)
            _patch_and_print_policy(pol, default_policy)


def _read_and_print_default_policy(in_text: io.TextIOWrapper) -> policy_.Policy:
    return _read_and_print_policy('Default', in_text)


def _read_and_print_policy(name: str, in_text: io.TextIOWrapper) -> policy_.Policy:
    _print_display_separator()
    result = policy_.Policy.from_json(in_text.read(), in_text.name)
    _print_policy_request(f'{name} policy: {in_text.name}', result)
    return result


def _read_policy_from_json(in_text: io.TextIOWrapper) -> policy_.Policy:
    return policy_.Policy.from_json(in_text.read(), in_text.name)


def _print_display_separator() -> None:
    print('#' * 20)


def _print_policy_request(title: str, value: Union[policy_.Policy, table.Sample]) -> None:
    pprint.pprint(f'{title}')
    pprint.pprint(attrs.asdict(value))


def _read_and_print_specific_policy(in_text: io.TextIOWrapper) -> policy_.Policy:
    return _read_and_print_policy('Specific', in_text)


def _patch_and_print_policy(policy_a: policy_.Policy, policy_b: policy_.Policy) -> policy_.Policy:
    # pylint: disable=protected-access
    result = sampler_bucket._overwrite_policy(policy_a, policy_b)
    _print_policy_request('Effective policy:', result)
    return result


def _list_dir(value: pathlib.Path) -> Generator[io.TextIOWrapper, None, None]:
    for f in _list_files_in_dir(value):
        yield open(f, "r", encoding="UTF-8")


def _list_files_in_dir(value: pathlib.Path) -> Generator[pathlib.Path, None, None]:
    for root, dirs, files in os.walk(value.absolute()):
        r_path = pathlib.Path(root)
        for f in files:
            yield r_path / f
        for d in dirs:
            val = r_path / d
            for res in _list_files_in_dir(val):
                yield res


def _read_request_for_policies(
    request_dir: pathlib.Path, policy_dir: pathlib.Path
) -> Generator[Tuple[policy_.Policy, table.Sample], None, None]:
    request_dir = request_dir.absolute()
    policy_dir = policy_dir.absolute()
    for req in _list_files_in_dir(request_dir):
        try:
            print(policy_dir)
            print(request_dir)
            print(req)
            pol_path = req.relative_to(request_dir).relative_to(policy_dir)
            with open(pol_path, encoding="UTF-8") as in_json:
                req_sample = _read_sample_from_json(in_json)
            with open(pol_path, encoding="UTF-8") as in_json:
                req_policy = _read_policy_from_json(in_json)
            yield req_policy, req_sample
        except ValueError as err:
            print(f"There is no policy for request {req}. Ignoring.")


def _read_sample_from_json(in_text: io.TextIOWrapper) -> table.Sample:
    return table.Sample.from_json(in_text.read(), in_text.name)


def _print_policy_request_pair(policy: policy_.Policy, sample: table.Sample) -> None:
    _print_display_separator()
    _print_policy_request("Specific policy", policy)
    _print_policy_request("Specific request", sample)


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
    result = table.Sample.from_json(in_text.read(), in_text.name)
    _print_policy_request(f'Sample request: {in_text.name}', result)
    return result


def _patch_and_print_request(
    request: table.Sample, request_policy: policy_.Policy, size: int
) -> table.Sample:
    # pylint: disable=protected-access
    effective_request = sampler_bucket._overwrite_request(request, request_policy)
    _print_policy_request('Effective request:', effective_request)
    result = request_policy.compliant_sample(effective_request, size)
    _print_policy_request('Effective sample:', result)
    return result


if __name__ == '__main__':
    cli()

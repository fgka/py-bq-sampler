# vim: ai:sw=4:ts=4:sta:et:fo=croql
"""
List all project IDs that should be sampled. It assumes the following structure in GCS bucket::
  /
    sampling-policy.json - the generic and assumed always right JSON file
    <PROJECT_ID>/
      <DATASET_ID>/
        <TABLE_ID>.json - contains the specific policy for this specific table that overwrites the generic, if valid.

"""
import logging
from typing import Any, Dict, List
from bq_sampler import gcp_storage


def get_all_policies(*, bucket_name: str, generic_policy_filename: str) -> List[Dict[str, Any]]:
    logging.info(
        'Retrieving all policies from bucket <%s> and using generic policy from <%s>',
        bucket_name,
        generic_policy_filename,
    )
    generic_policy
    return None

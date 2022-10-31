# vim: ai:sw=4:ts=4:sta:et:fo=croql
"""
Default values for creating an attributes class. To be used as::

    import attrs

    @attrs.define(**attrs_defaults.ATTRS_DEFAULTS)
    class MyAttrs: pass
"""
import re
from typing import Dict


###########
#  ATTRS  #
###########

ATTRS_DEFAULTS: Dict[str, bool] = dict(
    kw_only=True,
    str=True,
    repr=True,
    eq=True,
    hash=True,
    frozen=True,
    slots=True,
)


###################
#  Cloud Storage  #
###################

GS_PREFIX_DELIM: str = '/'

##########################
#  Samples and Policies  #
##########################

JSON_EXT: str = '.json'

SAMPLE_TYPE_RANDOM: str = 'random'
SAMPLE_TYPE_SORTED: str = 'sorted'

##############
#  BigQuery  #
##############

BQ_TABLE_FQN_ID_SEP: str = '.'
BQ_TABLE_FQN_LOCATION_SEP: str = '@'

BQ_ORDER_BY_ASC: str = 'ASC'
BQ_ORDER_BY_DESC: str = 'DESC'

DEFAULT_CREATE_TABLE_LABELS: Dict[str, str] = {
    'sample_table': 'true',
}
"""
Default GCP resource label to be applied table created here.
"""

#############
#  Command  #
#############

REQUEST_TYPE_START = 'START'
REQUEST_TYPE_SAMPLE_POLICY_PREFIX = 'SAMPLE_POLICY_PREFIX'
REQUEST_TYPE_SAMPLE_START = 'SAMPLE_START'
REQUEST_TYPE_SAMPLE_DONE = 'SAMPLE_DONE'
REQUEST_TYPE_TRANSFER_RUN_DONE = 'TRANSFER_RUN_DONE'
REQUEST_TYPE_REMOVE_DATASET = 'REMOVE_DATASET'


##################
#  Notification  #
##################

NOTIFICATION_PUBSUB_CONTENT_MESSAGE_TMPL: str = (
    'A new high or critical severity finding was detected:\n\n{}'
)

##############################
#  Transfer Run Notification #
##############################

TRANSFER_RUN_NAME_ATTR: str = 'name'
TRANSFER_RUN_DATA_SOURCE_ID_ATTR: str = 'dataSourceId'
TRANSFER_RUN_STATE_ATTR: str = 'state'
TRANSFER_RUN_STATE_SUCCEEDED_VALUE: str = 'SUCCEEDED'
# params
TRANSFER_RUN_PARAMS_ATTR: str = 'params'
TRANSFER_RUN_PARAMS_SOURCE_DATASET_ID_ATTR: str = 'source_dataset_id'
TRANSFER_RUN_PARAMS_SOURCE_PROJECT_ID_ATTR: str = 'source_project_id'
# parser
TRANSFER_CONFIG_FROM_RUN_NAME_RE: re.Pattern = re.compile(
    re.compile(r'^(projects/[^/\s]+/locations/[^/\s]+/transferConfigs/[^/\s]+)/runs/\S+$')
)
TRANSFER_CONFIG_NAME_RE: re.Pattern = re.compile(
    re.compile(r'^projects/([^/\s]+)/locations/([^/\s]+)/transferConfigs/([^/\s]+)$')
)

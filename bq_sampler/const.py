# vim: ai:sw=4:ts=4:sta:et:fo=croql
"""
Default values for creating an attributes class. To be used as::

    import attrs

    @attrs.define(**attrs_defaults.ATTRS_DEFAULTS)
    class MyAttrs: pass
"""

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
REQUEST_TYPE_SAMPLE_START = 'SAMPLE_START'
REQUEST_TYPE_SAMPLE_DONE = 'SAMPLE_DONE'

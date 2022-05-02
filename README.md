# Python code for BigQuery PROD Sampler

## Requirements

It requires [Python](https://www.python.org/) 3.9 or up.

### macOS:

* [Homebrew](https://brew.sh/);

```bash
brew install python3
```

## [Development](DEVELOPMENT.md)

## Install

```bash
python3 ./setup.py install
```

## Validating a request locally

### How to get help

General:

```bash
python -m bq_sampler --help
```

Policy:

```bash
python -m bq_sampler policy --help
```

Sample:

```bash
python -m bq_sampler sample-request --help
```

### Example validating a sample request

```bash
python -m bq_sampler sample-request \
  --default test_data/policy_bucket/default_policy.json \
  --policy test_data/policy_bucket/project_id_a/dataset_id_a/non_json.json \
  --request test_data/request_bucket/project_id_a/dataset_id_a/non_json.json \
  --size 10
```

Expected:

```text
####################
'Default policy: test_data/policy_bucket/default_policy.json'
{'default_sample': {'size': {'count': 10, 'percentage': 5.5},
                    'spec': {'properties': {'by': 'TEST_COLUMN_A',
                                            'direction': 'DESC'},
                             'type': 'sorted'}},
 'limit': {'count': 1000, 'percentage': 30.1}}
####################
Could not parse JSON string <{
  "__doc__": "The attribute default_sample is bad JSON, does not have the quotes around it",
  "limit": {
    "count": 1000,
    "percentage": 30.1
  },
  default_sample: {
    "size": {
      "count": 10,
      "percentage": 5.5
    },
    "spec": {
      "type": "sorted",
      "properties": {
        "by": "TEST_COLUMN_A",
        "direction": "DESC"
      }
    }
  }
}>. Ignoring. Error: Expecting property name enclosed in double quotes: line 7 column 3 (char 157)
('Specific policy: '
 'test_data/policy_bucket/project_id_a/dataset_id_a/non_json.json')
{'default_sample': None, 'limit': None}
'Effective policy:'
{'default_sample': {'size': {'count': 10, 'percentage': 5.5},
                    'spec': {'properties': {'by': 'TEST_COLUMN_A',
                                            'direction': 'DESC'},
                             'type': 'sorted'}},
 'limit': {'count': 1000, 'percentage': 30.1}}
####################
####################
('Sample request: '
 'test_data/request_bucket/project_id_a/dataset_id_a/non_json.json')
{'size': {'count': 18, 'percentage': 11.2}, 'spec': None}
'Effective request:'
{'size': {'count': 18, 'percentage': 11.2},
 'spec': {'properties': {'by': 'TEST_COLUMN_A', 'direction': 'DESC'},
          'type': 'sorted'}}
'Effective sample:'
{'size': {'count': 3, 'percentage': None},
 'spec': {'properties': {'by': 'TEST_COLUMN_A', 'direction': 'DESC'},
          'type': 'sorted'}}
####################
```
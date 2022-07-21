# Python code for BigQuery PROD Sampler

## Requirements

It requires [Python](https://www.python.org/) 3.9 or up.

### macOS:

* [Homebrew](https://brew.sh/);

```bash
brew install python3
```

## Dependencies

You will need:

- [Bash](https://www.gnu.org/software/bash/) (tested with `v3.2.57`)
- [bq](https://cloud.google.com/bigquery/docs/bq-command-line-tool) (tested with `v2.0.74`)
- [gnu-sed](https://www.gnu.org/software/sed/) (tested with `v4.8`) (on macOS the native one does not work)
- [gnu-getopt](https://www.gnu.org/software/libc/manual/html_node/Getopt.html) (tested with `v2.38`) (on macOS the native one does not work)
- [Google Cloud SDK](https://cloud.google.com/sdk) (tested with `v384.0.0`)
- [gsutil](https://cloud.google.com/storage/docs/gsutil) (tested with `v5.6`)
- [jq](https://stedolan.github.io/jq/) (tested with `v1.6`)
- [terraform](https://www.terraform.io/) (tested with `v1.1.9`)

## [Development](DEVELOPMENT.md)

## [Infrastructure](INFRASTRUCTURE.md)

### [Deploy](DEPLOY.md)

### [Integration Tests](INTEG_TESTING.md)

## Install

```bash
python3 ./setup.py install
```

## [CLI usage](CLI_USAGE.md)

## [Design](DESIGN.md)

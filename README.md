# Python code for BigQuery PROD Sampler

A more palatable discussion on design and purpose can be read in blog posts below:
* [Get your BigQuery production sample, all self-serving - Part 1](https://cloud.google.com/blog/products/data-analytics/speed-up-your-data-science-with-bigquery-sampling-from-prod)
* [Get your BigQuery production sample, all self-serving - Part 2](https://cloud.google.com/blog/products/data-analytics/self-service-production-sample-data-from-bigquery)

## [Code](./code)

Inside `code` you have also the instructions on how to manually deploy the infrastructure
 and manually test it end-to-end.
Main documents:

* [Development](./code/DEVELOPMENT.md)
* [CLI usage](./code/CLI_USAGE.md)
* [Integration Tests](./code/INTEG_TESTING.md)

## [Design](./DESIGN.md)

To understand the code and how it works, check out the [design documentation](./DESIGN.md).

## [Terraform](./terraform)

Here you will have the Terraform code to automatically deploy your infrastructure.
Just follow the instructions in the [README.md](./terraform/README.md) file.

You will also find:

* [Deploy](./terraform/DEPLOY.md)
* [Manual deploy](./terraform/DEPLOY_MANUAL.md)
* [End-to-End Testing](./terraform/TESTING.md)

## [License](./LICENSE)

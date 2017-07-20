[![Build Status](https://travis-ci.org/ocadotechnology/gcp-census.svg?branch=master)](https://travis-ci.org/ocadotechnology/gcp-census)
[![Coverage Status](https://coveralls.io/repos/github/ocadotechnology/gcp-census/badge.svg?branch=master)](https://coveralls.io/github/ocadotechnology/gcp-census?branch=master)
# gcp-census
GAE python based app which regularly collects information about GCP resources and stores them in BigQuery

Currently it supports only collecting metadata about the BigQuery tables (including [partitions summary](https://cloud.google.com/bigquery/docs/creating-partitioned-tables#listing_partitions_in_a_table)).

# Installation

Install dependencies:

```
pip install -t lib -r requirements.txt
pip install -r requirements_tests.txt
```

[![Build Status](https://travis-ci.org/ocadotechnology/gcp-census.svg?branch=master)](https://travis-ci.org/ocadotechnology/gcp-census)
[![Coverage Status](https://coveralls.io/repos/github/ocadotechnology/gcp-census/badge.svg?branch=master)](https://coveralls.io/github/ocadotechnology/gcp-census?branch=master)
# gcp-census
GAE python based app which regularly collects metadata about BigQuery tables and stores it in BigQuery.

GCP Census was created to answer the following questions:
* How much data we have in the whole GCP organisation?
* Which datasets/tables are the largest or most expensive?
* How many tables/partitions do we have?
* How often tables/partitions are updated over time?

Now every question above can be easily answered by querying metadata in BigQuery or looking at our dashboard created in [Google Data Studio](https://cloud.google.com/data-studio/).

## How it works?

![Architecture diagram](architecture-diagram.png)

GCP Census retrieves BigQuery metadata using [REST API](https://cloud.google.com/bigquery/docs/reference/rest/v2/):
1. Daily run is triggered by GAE cron (see [cron.yaml](config/cron.yaml) for exact details)
1. GCP Census iterates over all projects/datasets/tables to which it has access
1. A task is created for each table and queued for execution in GAE Task Queue
1. Task worker retrieves [Table metadata](https://cloud.google.com/bigquery/docs/reference/rest/v2/tables) and streams it into [bigquery.table_metadata_v0_1](bq_schemas/bigquery/table_metadata_v0_1.json) table. In case of partitioned tables, GCP Census retrieves also [partitions summary](https://cloud.google.com/bigquery/docs/creating-partitioned-tables#listing_partitions_in_a_table) by querying the partitioned table
1. User can query metadata using BigQuery UI/API
1. Optionally you can create a Data Studio dashboard based on metadata

GCP Census will retrieve metadata of tables it has read access to, which means the scope is derived from GCP IAM settings.

# Setup

1. Create GCP project and assign billing to it
1. Clone GCP Census repository
1. Specify metadata output BigQuery location in [app.yaml](app.yaml) (defaults to 'EU')
1. Install dependencies (ideally using [virtualenv](https://virtualenv.pypa.io/en/stable/)):
    ```
    pip install -r requirements.txt
    pip install -t lib -r requirements.txt
    ```
1. Deploy to App Engine using [gcloud](https://cloud.google.com/sdk/) CLI tool:
    ```
    gcloud app deploy --project YOUR-PROJECT-ID -v v1 app.yaml config/cron.yaml config/queue.yaml 
    ```
1. Grant [bigquery.dataViewer](https://cloud.google.com/bigquery/docs/access-control#bigquery.dataViewer) role to YOUR-PROJECT-ID@appspot.gserviceaccount.com service account at GCP organisation, folder or selected projects level.
1. Enable BigQuery in the project. BigQuery is automatically enabled in new projects. To activate it in a pre-existing project, enable the [BigQuery API](https://console.cloud.google.com/flows/enableapi?apiid=bigquery&_ga=2.23989375.-436268528.1473069416&_gac=1.245575088.1505292315.EAIaIQobChMI1OWh4OKh1gIVzbvtCh1GcwuwEAAYASAAEgL3sPD_BwE).
1. GCP Census job will be triggered daily by cron, see [cron.yaml](config/cron.yaml) for exact details
1. Optionally you can manually trigger cron jobs in [the Cloud Console](https://console.cloud.google.com/appengine/taskqueues/cron?tab=CRON):
    * run `/createModels` to create BigQuery dataset and table
    * run `/bigQuery` to start collecting BigQuery metadata

# Security

GCP Census endpoints are accessible only for GAE Administrators, i.e. all endpoints are secured with [login: admin](https://cloud.google.com/appengine/docs/standard/python/config/appref#handlers_login) in [app.yaml](app.yaml). 
Still, anyone may attempt to access your app and will be redirected to Google Account login page.

That's why we strongly recommend enabling [GAE Firewall](https://cloud.google.com/appengine/docs/standard/python/creating-firewalls) on your project.
You can enable it with three simple gcloud commands:
```
gcloud app firewall-rules create 500 --action allow --source-range 0.1.0.1 --description "Allow GAE cron" --project YOUR-PROJECT-ID
gcloud app firewall-rules create 510 --action allow --source-range 0.1.0.2 --description "Allow GAE tasks" --project YOUR-PROJECT-ID
gcloud app firewall-rules update default --action deny --project YOUR-PROJECT-ID
```

# Development

You can find all development setup/steps in [.travis.yml](.travis.yml)


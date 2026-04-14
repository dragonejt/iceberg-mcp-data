# iceberg-mcp-data
[![deploy](https://github.com/dragonejt/iceberg-mcp-data/actions/workflows/deliver.yml/badge.svg)](https://github.com/dragonejt/iceberg-mcp-data/actions/workflows/deliver.yml)

## Pipeline Overview

iceberg-mcp-data is a data pipeline that ingests PyPI file downloads and distribution metadata for [iceberg-mcp-server](https://github.com/dragonejt/iceberg-mcp-server), and then performs ETL and data processing on it. This data pipeline consists of the following components:

1. **Ingestion**: The `ingestions/` folder contains Spark Jobs that ingest external data and output as Iceberg tables in Databricks.

2. **Transformation**: The `transformations/` folder contains the Lakeflow Spark Declarative Pipeline that performs Spark ETL on those tables.

## Local Development

Local development with iceberg-mcp-data is done using the [Databricks Extension for VS Code]((https://marketplace.visualstudio.com/items?itemName=databricks.databricks)).

### Building and Running

1. With this project opened in VS Code, go to the Databricks sidebar.

2. Under Bundle Resource Explorer, click on the first button on the `[dev username] iceberg-mcp-data` pipeline labeled "Deploy the bundle and run the pipeline".

3. This will build and upload the project's [Declarative Automation Bundle](https://docs.databricks.com/aws/en/dev-tools/bundles/), and then run it on the Databricks platform.

### Testing

Testing involves both validating the built Declarative Automation Bundle and running PySpark unit tests.

### Unit Tests

This repository uses pytest for test running, although the tests themselves are structured in the unittest format. Running tests involves invoking pytest like any other project. If you use VS Code or a fork for development, the [VS Code Python Extension](https://marketplace.visualstudio.com/items?itemName=ms-python.python) will enable automatic test discovery and running in the Testing sidebar.

### Bundle Validation

Databricks Declarative Automation Bundles can be validated before they are deployed to ensure that the deployment and resource creation will succeed.

1. With this project opened in VS Code, go to the Databricks sidebar.

2. Under Bundle Resource Explorer, click on the third button on the `[dev username] iceberg-mcp-data` pipeline labeled "Deploy the bundle and validate the pipeline".

3. This will build and upload the project's [Declarative Automation Bundle](https://docs.databricks.com/aws/en/dev-tools/bundles/), and then run it on the Databricks platform.

### Linting and Formatting

iceberg-mcp-server uses [Ruff](https://docs.astral.sh/ruff/) and [ty](https://docs.astral.sh/ty/) for linting, formatting, and type checking. The standard commands to run are:
```bash
ruff check --fix # linting
ruff format # formatting
ty check # type checking
```
The Ruff configuration is found in `pyproject.toml`, and all autofixable issues will be autofixed. If you use VS Code or a fork for development, the [VS Code Ruff Extension](https://marketplace.visualstudio.com/items?itemName=charliermarsh.ruff) and [VS Code ty Extension](https://marketplace.visualstudio.com/items?itemName=astral-sh.ty) will enable viewing issues from Ruff and ty within your editor.

## Deployment

For deployment, iceberg-mcp-data is built into a Declarative Automation Bundle and deployed to Databricks.

### Continuous Delivery

iceberg-mcp-data has a continuous delivery GitHub Actions workflow, `deliver.yml`. The steps taken are summarized:

1. Setup the Databricks CLI.
2. Run `databricks bundle deploy` to package and deploy the Declarative Automation Bundle to Databricks.

Once deployed, the Ingestion Job and Lakeflow Spark Declarative Pipeline run daily in production.
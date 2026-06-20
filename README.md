# iceberg-mcp-data
[![codacy](https://app.codacy.com/project/badge/Grade/f376a00b8a334c118d2f5205aa052c72)](https://app.codacy.com/gh/dragonejt/iceberg-mcp-data/dashboard?utm_source=gh&utm_medium=referral&utm_content=&utm_campaign=Badge_grade)
[![coverage](https://app.codacy.com/project/badge/Coverage/f376a00b8a334c118d2f5205aa052c72)](https://app.codacy.com/gh/dragonejt/iceberg-mcp-data/dashboard?utm_source=gh&utm_medium=referral&utm_content=&utm_campaign=Badge_coverage)
[![integrate](https://github.com/dragonejt/iceberg-mcp-data/actions/workflows/integrate.yml/badge.svg)](https://github.com/dragonejt/iceberg-mcp-data/actions/workflows/integrate.yml)
[![pipeline](https://github.com/dragonejt/iceberg-mcp-data/actions/workflows/pipeline.yml/badge.svg)](https://github.com/dragonejt/iceberg-mcp-data/actions/workflows/pipeline.yml)

## Pipeline Overview

iceberg-mcp-data is a data pipeline that ingests PyPI file downloads and distribution metadata for [iceberg-mcp-server](https://github.com/dragonejt/iceberg-mcp-server), and then performs ETL and data processing on it. This data pipeline consists of the following components:

1. **Transformation**: The `transformations/` folder contains the Spark jobs that run using Snowpark Connect for Apache Spark.

2. **Notebooks**: The `notebooks/` folder contains Jupyter notebooks that interact with the data on Snowflake using Apache Iceberg and Snowflake Connect for Apache Spark.

## Local Development

Local development with iceberg-mcp-data is done using the [Snowflake Extension for VS Code](https://marketplace.visualstudio.com/items?itemName=snowflake.snowflake-vsc).

### Building and Running

1. Install all packages and set up the virtual environment by running `uv sync`.

2. Run the `pipeline` command to run the entire data pipeline on Snowpark Connect for Apache Spark using your Snowflake connection. This will run all transformation jobs in order.

3. Run the `pipeline <transform-name>` to run a specific transformation.

### Testing

This repository uses pytest for test running, although the tests themselves are structured in the unittest format. Running tests involves invoking pytest like any other project. If you use VS Code or a fork for development, the [VS Code Python Extension](https://marketplace.visualstudio.com/items?itemName=ms-python.python) will enable automatic test discovery and running in the Testing sidebar. Tests will also be run with coverage in the integration workflow.

### Linting and Formatting

iceberg-mcp-server uses [Ruff](https://docs.astral.sh/ruff/) and [ty](https://docs.astral.sh/ty/) for linting, formatting, and type checking. The standard commands to run are:
```bash
ruff check --fix # linting
ruff format # formatting
ty check # type checking
```
The Ruff configuration is found in `pyproject.toml`, and all autofixable issues will be autofixed. If you use VS Code or a fork for development, the [VS Code Ruff Extension](https://marketplace.visualstudio.com/items?itemName=charliermarsh.ruff) and [VS Code ty Extension](https://marketplace.visualstudio.com/items?itemName=astral-sh.ty) will enable viewing issues from Ruff and ty within your editor. Additionally, Ruff, ty, and CodeQL analysis will be run in the integration workflow.

## Orchestration

In production, pipeline workflow runs daily to orchestrate the data pipeline. The workflow can also be started manually for manual testing. Production data is written into `DEFAULT.PUBLIC`.

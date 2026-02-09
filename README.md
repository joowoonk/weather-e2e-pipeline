# Weather E2E Pipeline

A small end-to-end weather data pipeline using Snowflake Snowpark.

Overview
- Small ETL-style project that reads a source weather table, applies transformations, and writes derived tables.
- Key modules:
  - `project/utils.py` — environment/config helpers
  - `project/transformers.py` — transformation functions (modular)
  - `project/sproc.py` — orchestration script (Snowpark session + job)
  - `setup/create_table.py` — utilities for creating seed/source tables

Repository structure
```
project/
  utils.py
  transformers.py
  sproc.py
setup/
  create_table.py
.gitignore
README.md
```

Prerequisites
- Python 3.9+ (match Snowpark support)
- `snowflake-snowpark-python` installed in your environment

Recommended install
```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install snowflake-snowpark-python
```

Environment variables
This project expects Snowflake connection configuration via environment variables (set these in your shell or CI):
- `SNOWSQL_USER`
- `SNOWSQL_PWD`
- `SNOWSQL_ACCOUNT`
- `SNOWSQL_ROLE` (optional but recommended)
- `SNOWSQL_WAREHOUSE`
- `SNOWSQL_DATABASE`
- `SNOWSQL_SCHEMA`

Quickstart: run the job locally
1. Activate your environment and ensure env vars are set.
2. Run the orchestration script that creates fact tables:
```bash
python -m project.sproc
```
Note: `project/sproc.py` uses `Session.builder.configs(get_env_var_config()).create()` to build a Snowpark session.

Create seed/source tables
Use the helpers in `setup/create_table.py` to create or refresh the source `WEATHER` table in Snowflake. Example usage depends on your Snowpark session setup — open the file for details.

Testing & development notes
- Transformations are modular in `project/transformers.py`. Implement unit tests by mocking Snowpark DataFrame behavior or using small integration runs against a dev Snowflake DB.
- Add a `requirements.txt` if you want pinned deps for CI.

Next tasks (suggested)
- Implement a `query_service` to centralize loading/reading queries and add example calls to this README.
- Add tests for `transform_weather()` and a small sample dataset for deterministic runs.
- Add CI workflow to run linting and tests.

Contact
If you want help finishing `query_service` or adding tests, open an issue or ask here and I can implement a checklist and starter code.

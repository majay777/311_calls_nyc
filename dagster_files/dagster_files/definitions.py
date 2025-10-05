from .assets import calls, dbt
from dagster import Definitions, load_assets_from_modules
from .resources import dbt_resource
import os
from .resources.postgres_io import PostgresIOManager
call_assets = load_assets_from_modules([calls, dbt])

PSQL_CONFIG = {
    "host": os.getenv("DAGSTER_PG_HOST"),
    "port": os.getenv("POSTGRES_PORT"),
    "database": os.getenv("DAGSTER_PG_DB"),
    "user": os.getenv("DAGSTER_PG_USERNAME"),
    "password": os.getenv("DAGSTER_PG_PASSWORD"),
}

defs = Definitions(
    assets=[*call_assets],
    resources={
        "dbt": dbt_resource,
        "postgres_io": PostgresIOManager(PSQL_CONFIG),
    }
)
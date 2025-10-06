import os

from dagster import Definitions, load_assets_from_modules

from .assets import calls, dbt
from .jobs import nyc_311_job, nyc_311_dbt_job
from .resources import dbt_resource
from .resources.postgres_io import PostgresIOManager
from .schedules import nyc_311_Dbt_schedule, nyc_311_schedule

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
    },
    jobs=[nyc_311_job, nyc_311_dbt_job],
    schedules=[nyc_311_schedule, nyc_311_Dbt_schedule],
)
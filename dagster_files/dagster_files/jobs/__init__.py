from dagster import define_asset_job

from ..assets.calls import call_file, ingest_to_db
from ..assets.dbt import dbt_analytics, incremental_dbt_models

nyc_311_job = define_asset_job("nyc_311_job", selection=[call_file, ingest_to_db])

nyc_311_dbt_job = define_asset_job("nyc_311_dbt_job", selection=[dbt_analytics, incremental_dbt_models])

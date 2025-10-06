"""
To add a daily schedule that materializes your dbt assets, uncomment the following lines.
"""
from dagster import ScheduleDefinition, DefaultScheduleStatus

from ..jobs import nyc_311_dbt_job, nyc_311_job

# Addition: a ScheduleDefinition the job it should run and a cron schedule of how frequently to run it


nyc_311_schedule = ScheduleDefinition(
    job=nyc_311_job,
    cron_schedule="@daily",
    default_status=DefaultScheduleStatus.RUNNING,
)

nyc_311_Dbt_schedule = ScheduleDefinition(
    job=nyc_311_dbt_job,
    cron_schedule="0 1 * * *",
    default_status=DefaultScheduleStatus.RUNNING,
)

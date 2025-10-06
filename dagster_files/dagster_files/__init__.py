from .assets import calls, dbt
from dagster import Definitions, load_assets_from_modules

call_assets = load_assets_from_modules([calls, dbt])

defs = Definitions(
    assets=[*call_assets],

)

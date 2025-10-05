
from sqlalchemy import create_engine, inspect, MetaData, Column, String, Table, Integer,  BigInteger, Float, DateTime, \
    DECIMAL
from sqlalchemy.dialects.postgresql import insert
from smart_open import open

from dagster import asset, MaterializeResult, MetadataValue,  AssetExecutionContext
from datetime import datetime, timedelta
import requests
from io import BytesIO
from ..partitions import daily_partition
from ..resources import smart_open_config
import pandas as  pd
from .constants import  calls_file_path
import os

from ..resources.postgres_io import connect_postgres, PostgresIOManager

file_path1 = os.path.abspath(os.path.join("data", "raw","*.csv"))
user_name = os.getenv("DAGSTER_PG_USERNAME")
db_pwd = os.getenv("DAGSTER_PG_PASSWORD")
hostname = os.getenv("DAGSTER_PG_HOST")
dbname = os.getenv("DAGSTER_PG_DB")
port = os.getenv("POSTGRES_PORT")


@asset(group_name="api_call", compute_kind="Python", partitions_def=daily_partition,)
def call_file(context:  AssetExecutionContext) -> MaterializeResult:
    partition_date_str = context.partition_key
    date_to_fetch = partition_date_str
    current_date = date_to_fetch

    yesterdays_date = datetime.date(datetime.strptime(current_date, "%Y-%m-%d") - timedelta(1))
    file_path = os.path.abspath(calls_file_path.format(str(yesterdays_date)))
    api_endpoint = "https://data.cityofnewyork.us/resource/erm2-nwe9.csv?$where=created_date between '{}' and '{}'".format(str(yesterdays_date), str(current_date))
    calls_data = requests.get(api_endpoint)
    partition_date_str = context.partition_key
    month_to_fetch = partition_date_str[:-3]
    with open(file_path, "wb", transport_params=smart_open_config) as output_file:
        output_file.write(calls_data.content)
    num_rows = len(pd.read_csv(BytesIO(calls_data.content)))
    return MaterializeResult(metadata={"Number of records": MetadataValue.int(num_rows)})



@asset(group_name="ingest", deps=["call_file"], partitions_def=daily_partition, io_manager_key="postgres_io",
       required_resource_keys={"postgres_io"})
def ingest_to_db(context: AssetExecutionContext):
    global metadata
    partition_date_str = context.partition_key
    date_to_fetch = partition_date_str
    current_date = date_to_fetch

    yesterdays_date = datetime.date(datetime.strptime(current_date, "%Y-%m-%d") - timedelta(1))
    file_path = os.path.abspath(calls_file_path.format(str(yesterdays_date)))
    df = pd.read_csv(file_path)
    df = df.where(pd.notnull(df), None)
    # context.resources.postgres_io.extract_data()
    # engine = context.resources.postgres_io.connect()

    engine = create_engine("postgresql://{}:{}@{}:{}/{}".format(user_name,db_pwd,hostname,port,dbname))
    # Create an inspector
    inspector = inspect(engine)

    # Check if the table exists
    table_name = 'call_records'
    primarykey='unique_key'
    if table_name in inspector.get_table_names():
        connect = engine.connect()
        # connect = engine
        def insert_on_conflict_update(table, conn2, keys, data_iter):
            # update columns  on primary key conflict
            data = [dict(zip(keys, row)) for row in data_iter]
            stmt = insert(table.table).values(data)
            update_columns = {col.name: col for col in stmt.excluded if col.name not in primarykey}
            stmt = stmt.on_conflict_do_update(index_elements=[primarykey], set_=update_columns)
            result = conn2.execute(stmt)
            return result.rowcount
        df.to_sql(name=table_name, con=connect, if_exists="append", method=insert_on_conflict_update, index=False)
    else:
        metadata = MetaData()
        your_table = Table(
        table_name, metadata,
        Column('unique_key', Integer, primary_key=True),  # Primary key column
        Column('created_date', DateTime),
        Column('closed_date', DateTime),
        Column('agency', String),
        Column('agency_name', String),
        Column('complaint_type', String),
        Column('descriptor', String),
        Column('location_type', String),
        Column('incident_zip', DECIMAL),
        Column('incident_address', String),
        Column('street_name', String),
        Column('cross_street_1', String),
        Column('cross_street_2', String),
        Column('intersection_street_1', String),
        Column('intersection_street_2', String),
        Column('address_type', String),
        Column('city', String),
        Column('landmark', String),
        Column('facility_type', String),
        Column('status', String),
        Column('due_date', String),
        Column('resolution_description', String),
        Column('resolution_action_updated_date', String),
        Column('community_board', String),
        Column('bbl',DECIMAL),
        Column('borough', String),
        Column('x_coordinate_state_plane',DECIMAL),
        Column('y_coordinate_state_plane', DECIMAL),
        Column('open_data_channel_type', String),
        Column('park_facility_name', String),
        Column('park_borough', String),
        Column('vehicle_type', String),
        Column('taxi_company_borough', String),
        Column('taxi_pick_up_location', String),
        Column('bridge_highway_name', String),
        Column('bridge_highway_direction', String),
        Column('road_ramp', String),
        Column('bridge_highway_segment', String),
        Column('latitude',Float ),
        Column('longitude',Float),
        Column('location', String)
        )
        # Create the table in the database
        metadata.create_all(engine)
        df.to_sql(table_name, engine, if_exists='append', index=False, method='multi')

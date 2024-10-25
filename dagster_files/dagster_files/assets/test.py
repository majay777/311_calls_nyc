
import pandas as pd

import glob
import os

from Tools.scripts.make_ctype import method

# Define the path with the wildcard
path = r'D:\311_calls_nyc\dagster_files\data\raw\*.csv'

# Use glob to get all file paths
all_files = glob.glob(path)

# # Read each file and concatenate them into a single DataFrame
# df_list = []
# for file in all_files:
#     df = pd.read_csv(file)
#     df = df.fillna("NULL")
#     df_list.append(df)
#
# combined_df = pd.concat(df_list, ignore_index=True)
#
from sqlalchemy import create_engine, inspect, MetaData
from sqlalchemy.dialects.postgresql import insert
# engine = create_engine('postgresql://sqluser:sqluserpwd@localhost:5432/nyc_311_calls')
# combined_df.to_sql('call_records', engine, if_exists='append', index=False)


file_path = r'D:\311_calls_nyc\dagster_files\data\raw\call_records_2023-01-01.csv'
df = pd.read_csv(file_path)
df = df.where(pd.notnull(df), None)
# df_list = []
# for file in all_files:
#     df = pd.read_csv(file)
#     df = df.fillna("NULL")
#     df_list.append(df)
#
# combined_df = pd.concat(df_list, ignore_index=True)
# df = combined_df.where(pd.notnull(combined_df), None)


# combined_df.to_sql('call_records', engine, if_exists='append', index=False)
engine = create_engine('postgresql://sqluser:sqluserpwd@localhost:5432/nyc_311_calls')
# Create an inspector
inspector = inspect(engine)
conn = engine.connect()

print(inspector.get_table_names())
# Check if the table exists
table_name = 'call_records'
meta = MetaData()
meta.reflect(bind=engine)
table_used = meta.tables[table_name]
primarykey='unique_key'
table_name = 'call_records'
def insert_on_conflict_update(table, conn2, keys, data_iter):
    # update columns "b" and "c" on primary key conflict
    data = [dict(zip(keys, row)) for row in data_iter]
    stmt = insert(table.table).values(data)
    update_columns = {col.name: col for col in stmt.excluded if col.name not in primarykey}
    stmt = stmt.on_conflict_do_update(index_elements=[primarykey], set_=update_columns)
    result = conn2.execute(stmt)
    return result.rowcount
df.to_sql(name="call_records", con=conn, if_exists="append", method=insert_on_conflict_update, index=False)

print("In the if stmt: ")
# if table_name in inspector.get_table_names():
#     df.to_sql(table_name, engine,if_exists='append', index=False, method=postgres_upsert(conn), chunksize=1000)
    # print(table_name)
    # insrt_vals = df.to_dict(orient='records')
    # connect = engine.connect()
    # meta = MetaData()
    # meta.reflect(bind=engine)
    # table_used = meta.tables[table_name]
    # insrt_stmnt = insert(table_used).values(insrt_vals)
    # primarykey = 'unique_key'
    # update_columns = {col.name: col for col in insrt_stmnt.excluded if col.name not in primarykey}
    # upsert_stmt = insrt_stmnt.on_conflict_do_update(index_elements=[primarykey], set_=update_columns)
    # # Execute in batches to avoid timeouts
    # print("Here above batch code.")
    # batch_size = 1000
    # connect.execute(upsert_stmt)
    # # for i in range(0, len(insrt_vals), batch_size):
    #     batch = insrt_vals[i:i + batch_size]
    #     connect.execute(upsert_stmt, batch)


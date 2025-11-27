# import glob

# import pandas as pd
# path = r'D:\311_calls_nyc\dagster_files\data\raw\*.csv'

# # Use glob to get all file paths
# all_files = glob.glob(path)

# # Read each file and concatenate them into a single DataFrame
# df_list = []
# for file in all_files:
#     df = pd.read_csv(file)
#     df = df.fillna("NULL")
#     df_list.append(df)

# combined_df = pd.concat(df_list, ignore_index=True)
# print(combined_df.count())

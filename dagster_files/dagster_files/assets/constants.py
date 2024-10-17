import os



def get_path(path:str) -> str:
    return path


calls_file_path = get_path(os.path.join("data","raw", "call_records_{}.parquet"))
DATE_FORMAT = "%Y-%m-%d"

START_DATE = "2023-01-01"
END_DATE = "2023-04-01"
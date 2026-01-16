import pandas as pd
from s3_utils import read_csv_s3

AUTH_FILE = "auth_config.csv"

def load_password():
    df = read_csv_s3(AUTH_FILE, ["password"])
    if len(df) == 0:
        return None
    return str(df.iloc[0]["password"])

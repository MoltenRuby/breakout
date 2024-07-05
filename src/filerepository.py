import os
from pathlib import Path
from typing import Optional

import pandas as pd


def get_output_path():
    return Path(__file__).parents[1]/'out'


def save_data(df: pd.DataFrame, filename: str):
    os.makedirs(Path(filename).parent, exist_ok=True)
    df.to_csv(filename, header=True, index=False)


def get_data_from_repository(filename: Optional[str] = None):
    filename = filename or get_output_path() / 'screener.csv'
    return pd.read_csv(filename)

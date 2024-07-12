import os
from pathlib import Path
from typing import Optional

import pandas as pd


def get_output_path():
    return Path(__file__).parents[1]/'out'


def get_and_create_output_path(path=None):
    path = path or ''
    full_path = Path(__file__).parents[1]/'out'/path
    os.makedirs(full_path, exist_ok=True)
    return full_path


def save_data(df: pd.DataFrame, filename: str):
    os.makedirs(Path(filename).parent, exist_ok=True)
    df.to_csv(filename, header=True, index=False)


def get_data_from_repository(filename: Optional[str] = None):
    filename = filename or get_output_path() / 'screener.csv'
    return pd.read_csv(filename)

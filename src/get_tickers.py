# FROM https://github.com/shilewenuw/get_all_tickers/issues/15#issuecomment-830668105
import os
from pathlib import Path

import pandas as pd
import requests

from filerepository import get_output_path

_EXCHANGE_LIST = ['nyse', 'nasdaq', 'amex']

headers = {
    'authority': 'api.nasdaq.com',
    'accept': 'application/json, text/plain, */*',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36',
    'origin': 'https://www.nasdaq.com',
    'sec-fetch-site': 'same-site',
    'sec-fetch-mode': 'cors',
    'sec-fetch-dest': 'empty',
    'referer': 'https://www.nasdaq.com/',
    'accept-language': 'en-US,en;q=0.9',
}


def params(exchange):
    return (
        ('letter', '0'),
        ('exchange', exchange),
        ('render', 'download'),
    )


params = (
    ('tableonly', 'true'),
    ('limit', '25'),
    ('offset', '0'),
    ('download', 'true'),
)

def save_data(df, filename):
    os.makedirs(Path(filename).parent)
    df.to_csv(filename, header=False, index=False)

def fetch_data_df():
    r = requests.get('https://api.nasdaq.com/api/screener/stocks', headers=headers, params=params)
    data = r.json()['data']
    df = pd.DataFrame(data['rows'], columns=data['headers'])
    return df


def convert_string_price_to_float(value: str):
    return float(value.replace('$', ''))


def main():
    df = fetch_data_df()
    df['lastsale'] = df['lastsale'].apply(convert_string_price_to_float)
    output = get_output_path() / 'screener.csv'
    save_data(df, output)

if __name__ == '__main__':
    main()

# FROM https://github.com/shilewenuw/get_all_tickers/issues/15#issuecomment-830668105

import pandas as pd
import requests

from filerepository import get_output_path
from src.filerepository import save_data, get_data_from_repository

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


def fetch_data_df():
    r = requests.get('https://api.nasdaq.com/api/screener/stocks', headers=headers, params=params)
    data = r.json()['data']
    df = pd.DataFrame(data['rows'], columns=data['headers'])
    return df


def convert_string_price_to_float(value: str):
    return float(value.replace('$', ''))


def fetch_and_clean_data():
    df = fetch_data_df()
    df['lastsale'] = df['lastsale'].apply(convert_string_price_to_float)
    return df


def main():
    df = fetch_and_clean_data()
    output = get_output_path() / 'screener.csv'
    save_data(df, output)
    read_back = get_data_from_repository(output)
    pass


if __name__ == '__main__':
    main()

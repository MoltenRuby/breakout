import pandas as pd

import screener as scr
import src.filerepository
from src.filerepository import save_data
from filerepository import get_output_path

import yfinance as yf
from requests import Session
from requests_cache import CacheMixin, SQLiteCache
from requests_ratelimiter import LimiterMixin, MemoryQueueBucket
from pyrate_limiter import Duration, RequestRate, Limiter
from more_itertools import grouper

# KEYS = ['Open', 'High', 'Low', 'Close', 'Volume', 'Dividends', 'Stock Splits']

class CachedLimiterSession(CacheMixin, LimiterMixin, Session):
    pass


# TODO: remove ETF and indexes
def filter_symbol(symbol: str) -> bool:
    if '/' in symbol or '^' in symbol:
        return False
    return True

class Screener:
    def __init__(self):
        self.df = src.filerepository.get_data_from_repository()
        self.df = self.df.dropna(subset=['symbol'])

    @property
    def tickers(self):
        return list(filter(filter_symbol, self.df['symbol'].to_list()))

def download_data():
    screener = Screener()
    session = CachedLimiterSession(
        limiter=Limiter(RequestRate(2, Duration.SECOND * 5)),  # max 2 requests per 5 seconds
        bucket_class=MemoryQueueBucket,
        backend=SQLiteCache(get_output_path() / "yfinance.cache"),
    )

    chunks = grouper(screener.tickers, 200)

    for index, symbols in enumerate(chunks):
        try:
            tickers = yf.Tickers(list(symbols), session=session)
            df = tickers.download(period='1y')
            df.to_feather(get_feather_path(index))
        except Exception as e:
            print(f'Failed to save chunk {index} with exception {e}')

def load_data(index: int):
    return pd.read_feather(get_feather_path(index))


def get_feather_path(index):
    return get_output_path() / 'feather' / f'{index:05}-yfinance_download.feather'


def main():
    # download_data()

    df = load_data(0)
    all_cols = {col for col, _ in df.keys()}
    all_symbols = {symbol for _, symbol in df.keys()}

    pass


if __name__ == '__main__':
    main()

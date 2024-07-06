from pathlib import Path
from typing import Any

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
from dataclasses import dataclass
from datetime import datetime
from more_itertools import take

# keys = {'Low', 'Open', 'Stock Splits', 'High', 'Adj Close', 'Close', 'Dividends', 'Volume'}


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

def load_chunk_id(index: int):
    return pd.read_feather(get_feather_path(index))


def get_feather_root():
    return get_output_path() / 'feather'


def get_feather_path(index):
    return get_feather_root() / f'{index:05}-yfinance_download.feather'


def iter_chunks():
    yield from (file for file in Path(get_feather_root()).glob('**/*') if file.is_file())


@dataclass
class Datapoint:
    date: datetime
    value: float

@dataclass
class HighLow:
    symbol : str
    low: Datapoint
    high: Datapoint

def get_low_and_highs():
    for chunk in iter_chunks():
        df = pd.read_feather(chunk)
        # all_cols = {col for col, _ in df.keys()}
        symbols = {symbol for _, symbol in df.keys()}

        for symbol in symbols:
            highs = df[('High', symbol)]
            lows = df[('Low', symbol)]

            if not len(highs.dropna()) or not len(lows.dropna()):
                print(f'Skipping high and low for symbol {symbol} since no price data is available')
                continue

            yield HighLow(
                symbol=symbol,
                high=Datapoint(
                    date=highs.idxmax(axis=0),
                    value=highs[highs.idxmax(axis=0)]),
                low=Datapoint(
                    date=highs.idxmin(axis=0),
                    value=highs[highs.idxmin(axis=0)]),
                )


def main():
    # download_data()
    # for chunk in iter_chunks():
    #     print(chunk)

    symbols = []
    deltas = []
    for high_low in get_low_and_highs():
        symbols.append(high_low.symbol)

        delta = high_low.high.value / high_low.low.value if (
                high_low.low.date < high_low.high.date) else (
                high_low.low.value / high_low.high.value)
        deltas.append(delta)

    delta_df = pd.DataFrame({
        'Symbol': symbols,
        'Delta': deltas
    }).sort_values('Delta', ascending=False)

    for symbol in take(50, delta_df['Symbol'].values):
        print(symbol)

    # selected_tickers = delta_df.loc[delta_df['Symbol'].isin(['MSFT'])]






if __name__ == '__main__':
    main()

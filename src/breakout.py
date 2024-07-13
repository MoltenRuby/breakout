from functools import partial
from pathlib import Path
from typing import Any
import pickle

import pandas as pd
from datetime import timedelta

import screener as scr
from src.progress import tqdm_with_current
from throttle import Throttle
import src.filerepository
from src.filerepository import save_data
from filerepository import get_output_path, get_and_create_output_path

import yfinance as yf
import numpy as np
from requests import Session
from requests_cache import CacheMixin, SQLiteCache
from requests_ratelimiter import LimiterMixin, MemoryQueueBucket
from pyrate_limiter import Duration, RequestRate, Limiter
from more_itertools import grouper
from dataclasses import dataclass
from datetime import datetime
import secretsauce
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

def sort_biggest_winners():
    symbols = []
    deltas = []
    for high_low in get_low_and_highs():
        symbols.append(high_low.symbol)

        delta = high_low.high.value / high_low.low.value if (
                high_low.low.date < high_low.high.date) else (
                high_low.low.value / high_low.high.value)
        deltas.append(delta)

    delta_df = pd.DataFrame({
        'symbol': symbols,
        'delta': deltas
    }).sort_values('delta', ascending=False)

    delta_df = delta_df.loc[delta_df['delta'] > 1.5]
    return delta_df


def download_grades(symbols):
    throttle = Throttle(1.5e3)

    commands = [partial(secretsauce.get_report_card, chunk)
                for chunk in grouper(symbols, 100)]

    for index, result in enumerate(throttle.execute_all(commands)):
        with open(get_output_path() / f'{index:03}report_cards.pickle', 'wb') as file:
            pickle.dump(result, file)


def iter_grade_pickles():
    yield from (file for file in Path(get_output_path()).glob('*report_cards.pickle') if file.is_file())


def get_symbol_meta_data(symbol: str):
    ticker = yf.ticker.Ticker(symbol)
    return ticker.info


def split_industry_keys(keys: str):
    try:
        yield from keys.split('-')
    except Exception as e:
        print(f'Could not split industry key due to {e} for keys {keys}')


def is_industry_of_interest(industry_key: str) -> bool:
    industries_of_interest = {'electronic', 'health', 'development', 'scientific', 'computer', 'components', 'pharmaceutical', 'biotechnology', 'information', 'semiconductor', 'technology', 'internet', 'logistics', 'semiconductors' }

    return bool(set(industry_key.split('-')) and industries_of_interest)


def main():
    # download_data()
    # for chunk in iter_chunks():
    #     print(chunk)


###########
    # delta_df = sort_biggest_winners()
    with open(get_output_path() / 'delta_df.pickle', 'rb') as file:
        delta_df = pickle.load(file)
    delta_df = delta_df.rename(columns={'Symbol': 'symbol', 'Delta': 'delta'})
    # selected_tickers = delta_df.loc[delta_df['Symbol'].isin(['MSFT'])]
###################

    # download_grades(delta_df['Symbol'].dropna().values)

    all_grades = []
    for grade_file in iter_grade_pickles():
        with open(grade_file, 'rb') as file:
            all_grades.append(pickle.load(file))


    all_grades = [item for items in all_grades for item in items]
    all_grade_df = pd.DataFrame(all_grades)

    all_grade_df = pd.merge(all_grade_df, delta_df, on='symbol', how='outer').dropna()

    sort_order = [('total_grade', True), ('quant_grade', True), ('fund_grade', True), ('delta', False)]
    for order, ascending in reversed(sort_order):
        all_grade_df = all_grade_df.sort_values(by=order, ascending=ascending, kind='mergesort')

    symbols_metadata_folder = get_and_create_output_path('symbols_metadata')
    # symbols_metadata = [get_symbol_meta_data(symbol) for symbol in tqdm_with_current(all_grade_df['symbol'].values)]
    # with open(symbols_metadata_folder / 'symbols.pickle', 'wb') as file:
    #     pickle.dump(symbols_metadata, file)

    with open(symbols_metadata_folder / 'symbols.pickle', 'rb') as file:
        symbols_metadata = pickle.load(file)
    meta_df = pd.DataFrame(symbols_metadata)

    print(meta_df[['symbol', 'industry', 'industryKey', 'industryDisp', 'marketCap', 'sector', 'sectorKey', 'sectorDisp',
                   'heldPercentInsiders', 'heldPercentInstitutions']].to_string())

    all_industries = {industry_key for industry_keys in meta_df['industryKey'].dropna().values for industry_key in split_industry_keys(industry_keys)}

    # meta_df[is_industry_of_interest(meta_df['industryKey'])]


    print(all_grade_df.to_string())




if __name__ == '__main__':
    main()

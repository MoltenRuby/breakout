from typing import Iterable

import pandas as pd
import yfinance as yf
from tqdm import tqdm

from more_itertools import take


## Get Market cap:
# https://stackoverflow.com/questions/77728962/get-historic-market-cap-from-yfinance
# https://gist.github.com/vvksh/920828be6173c50133ab79d6fe7994af

def get_market_cap(ticker: "str") -> float:
    # print(f'Getting Market Cap for {ticker}')
    try:
        ticker_info = yf.Ticker(ticker)
        return ticker_info.info.get("marketCap", 0)
    except Exception as e:
        print(f'Failed to get Market cap for {ticker}. With error {e}')

def tqdm_with_current(iterable: Iterable):
    pbar = tqdm(iterable)
    for current in pbar:
        pbar.set_description(current)
        yield current

def createMarketCapCsv():
    with open('tickers.csv') as fd:
        tickers = [ticker.strip() for ticker in fd.readlines()]

    market_cap = [get_market_cap(ticker) for ticker in tqdm_with_current(tickers)]

    df = pd.DataFrame.from_dict({'Ticker': tickers, 'MarketCap': market_cap})
    df.to_csv('~/work/oracle/out/ticker_and_marketcap.csv')

    df.sort_values(by=['MarketCap'])
    df.to_csv('~/work/oracle/out/ticker_and_sorted_marketcap.csv')

def main():
    createMarketCapCsv()

    pass

if __name__ == '__main__':
    main()

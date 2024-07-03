# https://github.com/ranaroussi/yfinance/issues/1503
# https://stackoverflow.com/questions/71161902/get-info-on-multiple-stock-tickers-quickly-using-yfinance
# https://www.cboe.com/us/equities/market_statistics/listed_symbols/
# https://quant.stackexchange.com/questions/26162/where-can-i-get-a-list-of-all-yahoo-finance-stocks-symbols
# https://quant.stackexchange.com/questions/1640/where-to-download-list-of-all-common-stocks-traded-on-nyse-nasdaq-and-amex

# get info - https://stackoverflow.com/questions/71161902/get-info-on-multiple-stock-tickers-quickly-using-yfinance

#https://unibit.ai/api/docs/V2.0/stock_coverage
# ! https://www.nasdaq.com/market-activity/stocks/screener
# https://github.com/Benny-/Yahoo-ticker-symbol-downloader
import yfinance as yf
from yfinance import ticker, tickers, download
from get_all_tickers import get_tickers as gt
from ytd.SimpleSymbolDownloader import SymbolDownloader

def main():
    symbol_downloader = SymbolDownloader("generic")
    list_of_tickers = gt.get_tickers(NYSE=False, NASDAQ=False, AMEX=True)
    print(list_of_tickers)

if __name__ == '__main__':
    main()
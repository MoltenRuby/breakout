import screener as scr

import yfinance as yf

class Screener:
    def __init__(self):
        self.df = scr.get_data_from_repository()
        self.df = self.df.dropna(subset=['symbol'])

    @property
    def tickers(self):
        return self.df['symbol'].to_list()


def main():
    screener = Screener()
    tickers = yf.Tickers(screener.tickers)
    #tickers.tickers['AAPL'].info
    pass


if __name__ == '__main__':
    main()

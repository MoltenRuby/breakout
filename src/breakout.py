import screener as scr
import src.filerepository
from src.filerepository import save_data
from filerepository import get_output_path

import yfinance as yf
from requests import Session
from requests_cache import CacheMixin, SQLiteCache
from requests_ratelimiter import LimiterMixin, MemoryQueueBucket
from pyrate_limiter import Duration, RequestRate, Limiter

class CachedLimiterSession(CacheMixin, LimiterMixin, Session):
    pass



class Screener:
    def __init__(self):
        self.df = src.filerepository.get_data_from_repository()
        self.df = self.df.dropna(subset=['symbol'])

    @property
    def tickers(self):
        return self.df['symbol'].to_list()


def main():
    screener = Screener()

    session = CachedLimiterSession(
        limiter=Limiter(RequestRate(2, Duration.SECOND * 5)),  # max 2 requests per 5 seconds
        bucket_class=MemoryQueueBucket,
        backend=SQLiteCache(get_output_path() / "yfinance.cache"),
    )

    tickers = yf.Tickers(screener.tickers, session=session)
    # TODO: remove ETF and indexes
    #tickers.tickers['AAPL'].info
    data = tickers.download(period='1y')
    save_data(data, 'yfinance_download')


if __name__ == '__main__':
    main()

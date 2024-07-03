# FROM https://github.com/shilewenuw/get_all_tickers/issues/15#issuecomment-830668105
import pandas as pd
from enum import Enum
import io
import requests
from more_itertools import grouper

_ALL_SECTORS = {'', 'Utilities', 'Finance', 'Health Care', 'Consumer Discretionary',
                'Technology', 'Real Estate', 'Telecommunications',
                'Basic Materials', 'Energy', 'Consumer Staples', 'Miscellaneous',
                'Industrials'}

_ALL_INDUSTRIES = {'', 'Other Metals and Minerals', 'Building Materials',
                   'Military/Government/Technical', 'Retail: Computer Software & Peripheral Equipment',
                   'Business Services', 'Aluminum', 'Biotechnology: Laboratory Analytical Instruments',
                   'Hospital/Nursing Management', 'Retail-Drug Stores and Proprietary Stores', 'Trucking Freight/Courier Services',
                   'Marine Transportation', 'General Bldg Contractors - Nonresidential Bldgs', 'Major Chemicals',
                   'Auto Parts:O.E.M.', 'Medical Specialities', 'Homebuilding', 'Investment Managers',
                   'Clothing/Shoe/Accessory Stores', 'Air Freight/Delivery Services', 'Biotechnology: Commercial Physical & Biological Resarch',
                   'Medical/Nursing Services', 'Beverages (Production/Distribution)',
                   'Professional and commerical equipment', 'Home Furnishings',
                   'Computer peripheral equipment', 'Building Products', 'Real Estate Investment Trusts',
                   'Biotechnology: In Vitro & In Vivo Diagnostic Substances', 'Aerospace', 'Misc Health and Biotechnology Services',
                   'Ordnance And Accessories', 'Food Distributors', 'Auto & Home Supply Stores', 'Environmental Services', 'Advertising',
                   'Mining & Quarrying of Nonmetallic Minerals (No Fuels)', ' Medicinal Chemicals and Botanical Products ', 'Pollution Control Equipment',
                   'Medical Electronics', 'Integrated oil Companies', 'Savings Institutions', 'Package Goods/Cosmetics',
                   'Accident &Health Insurance', 'Water Supply', 'Packaged Foods', 'Tobacco', 'Electrical Products', 'Farming/Seeds/Milling',
                   'Books', 'Containers/Packaging', 'Catalog/Specialty Distribution', 'Investment Bankers/Brokers/Service', 'Finance/Investors Services',
                   'Telecommunications Equipment', 'Consumer Specialties', 'Precision Instruments', 'Meat/Poultry/Fish', 'Shoe Manufacturing',
                   'Agricultural Chemicals', 'Hotels/Resorts', 'Consumer Electronics/Appliances', 'Industrial Machinery/Components', 'EDP Services',
                   'RETAIL: Building Materials', 'Durable Goods', 'Electronic Components', 'Misc Corporate Leasing Services',
                   'Other Specialty Stores', 'Commercial Banks', 'Forest Products', 'Tools/Hardware', 'Specialty Foods', 'Rental/Leasing Companies',
                   'Finance Companies', 'Biotechnology: Electromedical & Electrotherapeutic Apparatus', 'Paints/Coatings', 'Oilfield Services/Equipment',
                   'Department/Specialty Retail Stores', 'Publishing', 'Engineering & Construction', 'Natural Gas Distribution', 'Oil/Gas Transmission',
                   'Broadcasting', 'Services-Misc. Amusement & Recreation', 'Restaurants', 'Miscellaneous manufacturing industries', 'Blank Checks',
                   'Multi-Sector Companies', 'Steel/Iron Ore', 'Diversified Commercial Services', 'Life Insurance',
                   'Computer Software: Programming Data Processing', 'Transportation Services', 'Electronics Distribution',
                   'Oil & Gas Production', 'Food Chains', 'Cable & Other Pay Television Services', 'Banks',
                   'Radio And Television Broadcasting And Communications Equipment', 'Auto Manufacturing', 'Industrial Specialties',
                   'Recreational Games/Products/Toys', 'Building operators', 'Property-Casualty Insurers', 'Wholesale Distributors',
                   'Fluid Controls', 'Oil Refining/Marketing', 'Professional Services', 'Computer Communications Equipment',
                   'Trusts Except Educational Religious and Charitable', 'Metal Mining', 'Paper', 'Real Estate', 'Electric Utilities: Central',
                   'Biotechnology: Biological Products (No Diagnostic Substances)', 'Pharmaceuticals and Biotechnology', 'Movies/Entertainment',
                   'Consumer Electronics/Video Chains', 'Specialty Insurers', 'Major Banks', 'Biotechnology: Pharmaceutical Preparations',
                   'Office Equipment/Supplies/Services', 'Motor Vehicles', 'Semiconductors', 'Ophthalmic Goods', 'Medical/Dental Instruments',
                   'Metal Fabrications', 'Other Consumer Services', 'Computer Software: Prepackaged Software', 'Computer Manufacturing', 'Textiles',
                   'Apparel', 'Power Generation', 'Railroads', 'Construction/Ag Equipment/Trucks', 'Automotive Aftermarket', 'Specialty Chemicals',
                   'Diversified Financial Services', 'Assisted Living Services', 'Finance: Consumer Services', 'Precious Metals',
                   'Water Sewer Pipeline Comm & Power Line Construction', 'Plastic Products', 'Other Pharmaceuticals', 'Newspapers/Magazines',
                   'Retail-Auto Dealers and Gas Stations', 'Coal Mining'}

_INDUSTRY_FILTER = {

                   'Military/Government/Technical',
                   'Business Services', 'Biotechnology: Laboratory Analytical Instruments',
                   'Major Chemicals',
                   'Biotechnology: Commercial Physical & Biological Resarch',


                   'Biotechnology: In Vitro & In Vivo Diagnostic Substances', 'Aerospace', 'Misc Health and Biotechnology Services',
                   'Advertising',

                   ' Medicinal Chemicals and Botanical Products ',
                   'Medical Electronics',
                   'Electrical Products',

                   'Telecommunications Equipment', 'Precision Instruments',
                   'Consumer Electronics/Appliances', 'Industrial Machinery/Components',
                   'Electronic Components',

                   'Biotechnology: Electromedical & Electrotherapeutic Apparatus',
                   'Engineering & Construction',

                   'Multi-Sector Companies', 'Steel/Iron Ore', 'Diversified Commercial Services', 'Life Insurance',
                   'Computer Software: Programming Data Processing', 'Transportation Services', 'Electronics Distribution',
                   'Oil & Gas Production', 'Food Chains', 'Cable & Other Pay Television Services', 'Banks',
                   'Radio And Television Broadcasting And Communications Equipment', 'Auto Manufacturing', 'Industrial Specialties',
                   'Recreational Games/Products/Toys', 'Building operators', 'Property-Casualty Insurers', 'Wholesale Distributors',
                   'Fluid Controls', 'Oil Refining/Marketing', 'Professional Services', 'Computer Communications Equipment',
                   'Trusts Except Educational Religious and Charitable', 'Metal Mining', 'Paper', 'Real Estate', 'Electric Utilities: Central',
                   'Biotechnology: Biological Products (No Diagnostic Substances)', 'Pharmaceuticals and Biotechnology', 'Movies/Entertainment',
                   'Consumer Electronics/Video Chains', 'Specialty Insurers', 'Major Banks', 'Biotechnology: Pharmaceutical Preparations',
                   'Office Equipment/Supplies/Services', 'Motor Vehicles', 'Semiconductors', 'Ophthalmic Goods', 'Medical/Dental Instruments',
                   'Metal Fabrications', 'Other Consumer Services', 'Computer Software: Prepackaged Software', 'Computer Manufacturing', 'Textiles',
                   'Apparel', 'Power Generation', 'Railroads', 'Construction/Ag Equipment/Trucks', 'Automotive Aftermarket', 'Specialty Chemicals',
                   'Diversified Financial Services', 'Assisted Living Services', 'Finance: Consumer Services', 'Precious Metals',
                   'Water Sewer Pipeline Comm & Power Line Construction', 'Plastic Products', 'Other Pharmaceuticals', 'Newspapers/Magazines',
                   'Retail-Auto Dealers and Gas Stations', 'Coal Mining'}


_EXCHANGE_LIST = ['nyse', 'nasdaq', 'amex']

_SECTORS_LIST = set(['Consumer Non-Durables', 'Capital Goods', 'Health Care',
                     'Energy', 'Technology', 'Basic Industries', 'Finance',
                     'Consumer Services', 'Public Utilities', 'Miscellaneous',
                     'Consumer Durables', 'Transportation'])

# headers and params used to bypass NASDAQ's anti-scraping mechanism in function __exchange2df
# headers = {
#     'authority': 'old.nasdaq.com',
#     'upgrade-insecure-requests': '1',
#     'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36',
#     'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
#     'sec-fetch-site': 'cross-site',
#     'sec-fetch-mode': 'navigate',
#     'sec-fetch-user': '?1',
#     'sec-fetch-dest': 'document',
#     'referer': 'https://github.com/shilewenuw/get_all_tickers/issues/2',
#     'accept-language': 'en-US,en;q=0.9',
#     'cookie': 'AKA_A2=A; NSC_W.TJUFEFGFOEFS.OBTEBR.443=ffffffffc3a0f70e45525d5f4f58455e445a4a42378b',
# }

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


def params_region(region):
    return (
        ('letter', '0'),
        ('region', region),
        ('render', 'download'),
    )


# I know it's weird to have Sectors as constants, yet the Regions as enums, but
# it makes the most sense to me
class Region(Enum):
    AFRICA = 'AFRICA'
    EUROPE = 'EUROPE'
    ASIA = 'ASIA'
    AUSTRALIA_SOUTH_PACIFIC = 'AUSTRALIA+AND+SOUTH+PACIFIC'
    CARIBBEAN = 'CARIBBEAN'
    SOUTH_AMERICA = 'SOUTH+AMERICA'
    MIDDLE_EAST = 'MIDDLE+EAST'
    NORTH_AMERICA = 'NORTH+AMERICA'


class SectorConstants:
    NON_DURABLE_GOODS = 'Consumer Non-Durables'
    CAPITAL_GOODS = 'Capital Goods'
    HEALTH_CARE = 'Health Care'
    ENERGY = 'Energy'
    TECH = 'Technology'
    BASICS = 'Basic Industries'
    FINANCE = 'Finance'
    SERVICES = 'Consumer Services'
    UTILITIES = 'Public Utilities'
    DURABLE_GOODS = 'Consumer Durables'
    TRANSPORT = 'Transportation'


# get tickers from chosen exchanges (default all) as a list
def get_tickers(NYSE=True, NASDAQ=True, AMEX=True):
    tickers_list = []
    if NYSE:
        tickers_list.extend(__exchange2list('nyse'))
    if NASDAQ:
        tickers_list.extend(__exchange2list('nasdaq'))
    if AMEX:
        tickers_list.extend(__exchange2list('amex'))
    return tickers_list


def get_tickers_filtered(mktcap_min=None, mktcap_max=None, sectors=None):
    tickers_list = []
    for exchange in _EXCHANGE_LIST:
        tickers_list.extend(
            __exchange2list_filtered(exchange, mktcap_min=mktcap_min, mktcap_max=mktcap_max, sectors=sectors))
    return tickers_list


def get_biggest_n_tickers(top_n, sectors=None):
    df = pd.DataFrame()
    for exchange in _EXCHANGE_LIST:
        temp = __exchange2df(exchange)
        df = pd.concat([df, temp])

    df = df.dropna(subset={'marketCap'})
    df = df[~df['symbol'].str.contains("\.|\^")]

    if sectors is not None:
        if isinstance(sectors, str):
            sectors = [sectors]
        if not _SECTORS_LIST.issuperset(set(sectors)):
            raise ValueError('Some sectors included are invalid')
        sector_filter = df['Sector'].apply(lambda x: x in sectors)
        df = df[sector_filter]

    def cust_filter(mkt_cap):
        if 'M' in mkt_cap:
            return float(mkt_cap[1:-1])
        elif 'B' in mkt_cap:
            return float(mkt_cap[1:-1]) * 1000
        else:
            try:
                return float(mkt_cap[1:]) / 1e6
            except:
                print(f'Could not get market cap from "{mkt_cap}"')

    df['marketCap'] = df['marketCap'].apply(cust_filter)

    df = df.sort_values('marketCap', ascending=False)
    if top_n > len(df):
        raise ValueError('Not enough companies, please specify a smaller top_n')

    return df.iloc[:top_n]['symbol'].tolist()


def get_tickers_by_region(region):
    if region in Region:
        response = requests.get('https://old.nasdaq.com/screening/companies-by-name.aspx', headers=headers,
                                params=params_region(region))
        data = io.StringIO(response.text)
        df = pd.read_csv(data, sep=",")
        return __exchange2list(df)
    else:
        raise ValueError('Please enter a valid region (use a Region.REGION as the argument, e.g. Region.AFRICA)')


def __exchange2df(exchange):
    # response = requests.get('https://old.nasdaq.com/screening/companies-by-name.aspx', headers=headers, params=params(exchange))
    # data = io.StringIO(response.text)
    # df = pd.read_csv(data, sep=",")
    r = requests.get('https://api.nasdaq.com/api/screener/stocks', headers=headers, params=params)
    data = r.json()['data']
    df = pd.DataFrame(data['rows'], columns=data['headers'])
    return df


def __exchange2list(exchange):
    df = __exchange2df(exchange)
    # removes weird tickers
    df_filtered = df[~df['symbol'].str.contains("\.|\^")]
    return df_filtered['symbol'].tolist()


# market caps are in millions
def __exchange2list_filtered(exchange, mktcap_min=None, mktcap_max=None, sectors=None):
    df = __exchange2df(exchange)
    # df = df.dropna(subset={'MarketCap'})
    df = df.dropna(subset={'marketCap'})
    df = df[~df['symbol'].str.contains("\.|\^")]

    if sectors is not None:
        if isinstance(sectors, str):
            sectors = [sectors]
        if not _SECTORS_LIST.issuperset(set(sectors)):
            raise ValueError('Some sectors included are invalid')
        sector_filter = df['sector'].apply(lambda x: x in sectors)
        df = df[sector_filter]

    def cust_filter(mkt_cap):
        if 'M' in mkt_cap:
            return float(mkt_cap[1:-1])
        elif 'B' in mkt_cap:
            return float(mkt_cap[1:-1]) * 1000
        elif mkt_cap == '':
            return 0.0
        else:
            return float(mkt_cap[1:]) / 1e6

    df['marketCap'] = df['marketCap'].apply(cust_filter)
    if mktcap_min is not None:
        df = df[df['marketCap'] > mktcap_min]
    if mktcap_max is not None:
        df = df[df['marketCap'] < mktcap_max]
    return df['symbol'].tolist()


# save the tickers to a CSV
def save_tickers(NYSE=True, NASDAQ=True, AMEX=True, filename='tickers.csv'):
    tickers2save = get_tickers(NYSE, NASDAQ, AMEX)
    df = pd.DataFrame(tickers2save)
    df.to_csv(filename, header=False, index=False)


def save_tickers_by_region(region, filename='tickers_by_region.csv'):
    tickers2save = get_tickers_by_region(region)
    df = pd.DataFrame(tickers2save)
    df.to_csv(filename, header=False, index=False)

def conver_string_price_to_float(value: str):
    return float(value.replace('$', ''))

if __name__ == '__main__':
    # tickers of all exchanges
    # tickers = get_tickers()
    # print(tickers[:5])


    nyse = __exchange2df('nyse')
    nasdaq = __exchange2df('nasdaq')
    amex = __exchange2df('amex')


    alldf = nyse.merge(nasdaq).merge(amex)
    alldf['lastsale'] = alldf['lastsale'].apply(conver_string_price_to_float, )
    alldf['marketCap'] = pd.to_numeric(alldf['marketCap'])

    print('-----In Range for Price---------')
    in_range = alldf.loc[alldf['lastsale'] > 50]
    in_range = in_range.loc[in_range['lastsale'] < 60]
    in_range[['symbol', 'lastsale']]
    in_range_symbols = in_range[['symbol']].values.flatten()

    for tik in in_range_symbols:
        print(tik)
    print('--------------------------------')

    print('-------Inspect Tickers----------')
    ticker_filter = {'CLS', 'FORM', 'SMCI', 'TXG', 'EXAI', 'SYM', 'CRSP'}
    selected_tickers = alldf.loc[alldf['symbol'].isin(ticker_filter)]
    # selected_tickers[['symbol', 'sector', 'industry', 'marketCap']]
    print('--------------------------------')

    print('-----In Range for Market Cap----')
    in_market_cap_range = alldf.loc[alldf['marketCap'] > 1]
    in_market_cap_range = in_market_cap_range.loc[in_market_cap_range['marketCap'] < 100*1e6]

    # sector_filter = {
    #     # 'Health Care', 'Consumer Discretionary',
    #                 'Technology',
    #                 # 'Basic Materials', 'Energy',
    #                  }
    #
    # in_market_cap_range = in_market_cap_range.loc[in_market_cap_range['sector'].isin(sector_filter)]



    missing_market_cap = alldf.loc[alldf['marketCap'] == 0]['symbol']

    chunks = grouper(in_market_cap_range[['symbol']].values.flatten(), 100, incomplete='ignore')

    for index, chunk in enumerate(chunks):
        with open(f'/home/alex/work/oracle/out/market-cap-filter-chunk-{index}.txt', mode='w') as fd:
            for tik in chunk:
                print(tik, file=fd)

    print('--------------------------------')

    # nyse.loc[nyse['marketCap'] == 0] # get 0 market cap -> must conver to float
    pass
    # tickers from NYSE and NASDAQ only
    # tickers = get_tickers(AMEX=False)

    # default filename is tickers.csv, to specify, add argument filename='yourfilename.csv'
    # save_tickers()

    # save tickers from NYSE and AMEX only
    # save_tickers(NASDAQ=True)

    # get tickers from Asia
    # tickers_asia = get_tickers_by_region(Region.ASIA)
    # print(tickers_asia[:5])

    # save tickers from Europe
    # save_tickers_by_region(Region.EUROPE, filename='EU_tickers.csv')

    # get tickers filtered by market cap (in millions)
    # filtered_tickers = get_tickers_filtered(mktcap_min=500, mktcap_max=2000)
    # print(filtered_tickers[:5])
    #
    # # not setting max will get stocks with $2000 million market cap and up.
    # filtered_tickers = get_tickers_filtered(mktcap_min=2000)
    # print(filtered_tickers[:5])
    #
    # # get tickers filtered by sector
    # filtered_by_sector = get_tickers_filtered(mktcap_min=200e3, sectors=SectorConstants.FINANCE)
    # print(filtered_by_sector[:5])
    #
    # # get tickers of 5 largest companies by market cap (specify sectors=SECTOR)
    # top_5 = get_biggest_n_tickers(5)
    # print(top_5)
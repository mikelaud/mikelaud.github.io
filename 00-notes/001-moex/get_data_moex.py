from enum import Enum, IntEnum, unique
import json
import requests


@unique
class DataFormat(Enum):
    CSV  = 'csv'
    HTML = 'html'
    JSON = 'json'
    XML  = 'xml'


@unique
class DataInterval(Enum):
    M1   = '1',  # 1 minute
    M10  = '10', # 10 minutes
    H1   = '60', # 1 hour
    D1   = '24', # 1 day
    W1   = '7',  # 1 week
    MON1 = '31', # 1 month
    MON3 = '4'   # 3 months


@unique
class Symbol(IntEnum):
    AFLT  = 1,
    ALRS  = 2,
    CHMF  = 3,
    FEES  = 4,
    GAZP  = 5,
    GMKN  = 6,
    HYDR  = 7,
    IRAO  = 8,
    LKOH  = 9,
    MGNT  = 10,
    MOEX  = 11,
    MTSS  = 12,
    NLMK  = 13,
    NVTK  = 14,
    PLZL  = 15,
    POLY  = 16,
    ROSN  = 17,
    SBER  = 18,
    SBERP = 19,
    SNGS  = 20,
    SNGSP = 21,
    TATN  = 22,
    TATNP = 23,
    VTBR  = 24,
    YNDX  = 25


@unique
class SymbolBlue(IntEnum):
    """ MOEXBC 2021.07.16 (-FIVE)
    https://www.moex.com/en/index/MOEXBC
    """
    GAZP  = Symbol.GAZP,
    GMKN  = Symbol.GMKN,
    LKOH  = Symbol.LKOH,
    MGNT  = Symbol.MGNT,
    MTSS  = Symbol.MTSS,
    NLMK  = Symbol.NLMK,
    NVTK  = Symbol.NVTK,
    PLZL  = Symbol.PLZL,
    POLY  = Symbol.POLY,
    ROSN  = Symbol.ROSN,
    SBER  = Symbol.SBER,
    SNGS  = Symbol.SNGS,
    TATN  = Symbol.TATN,
    YNDX  = Symbol.YNDX 


@unique
class SymbolNight(IntEnum):
    """ On June 22, 2020, the following shares are allowed to trade at the evening trading session:
    https://www.moex.com/n28495
    https://www.moex.com/n29024
    """
    AFLT  = Symbol.AFLT,
    ALRS  = Symbol.ALRS,
    CHMF  = Symbol.CHMF,
    FEES  = Symbol.FEES,
    GAZP  = Symbol.GAZP,
    GMKN  = Symbol.GMKN,
    HYDR  = Symbol.HYDR,
    IRAO  = Symbol.IRAO,
    LKOH  = Symbol.LKOH,
    MGNT  = Symbol.MGNT,
    MOEX  = Symbol.MOEX,
    MTSS  = Symbol.MTSS,
    NLMK  = Symbol.NLMK,
    NVTK  = Symbol.NVTK,
    PLZL  = Symbol.PLZL,
    POLY  = Symbol.POLY,
    ROSN  = Symbol.ROSN,
    SBER  = Symbol.SBER,
    SBERP = Symbol.SBERP,
    SNGS  = Symbol.SNGS,
    SNGSP = Symbol.SNGSP,
    TATN  = Symbol.TATN,
    TATNP = Symbol.TATNP,
    VTBR  = Symbol.VTBR,
    YNDX  = Symbol.YNDX


def get_url(symbol, data_format=DataFormat.JSON):
    url_template = 'https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities/{symbol}/candles.{data_format}'
    return url_template.format(symbol=symbol.name, data_format=data_format.value)


def get_data(symbol):
    url = get_url(symbol)
    print('URL: {url}'.format(url=url))
    params = {
        'from'    : '2021-07-16',
        'till'    : '2021-07-16',
        'interval': DataInterval.M1.value,
        'start'   : 500
    }
    resp = requests.get(url=url, params=params)
    data = resp.json()
    #print(json.dumps(data, indent=4))
    print(data)


def main():
    print('Get data from MOEX...')
    get_data(SymbolBlue.SBER)
    print('Done.')


if __name__ == "__main__":
    main()


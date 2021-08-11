"""
MOEX data downloader based on:
    Programming interface to ISS
    - https://www.moex.com/a2193
    - http://iss.moex.com/iss/reference/
    - https://fs.moex.com/files/6523
    - https://fs.moex.com/files/6524
"""

from datetime import datetime
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
    M1   = '1'  # 1 minute
    M10  = '10' # 10 minutes
    H1   = '60' # 1 hour
    D1   = '24' # 1 day
    W1   = '7'  # 1 week
    MON1 = '31' # 1 month
    MON3 = '4'  # 3 months


@unique
class Engine(Enum):
    """
    https://iss.moex.com/iss/engines
    https://iss.moex.com/iss/engines.xml
    https://iss.moex.com/iss/engines.json
    """
    STOCK         = 'stock'
    STATE         = 'state'
    CURRENCY      = 'currency'
    FUTURES       = 'futures'
    COMMODITY     = 'commodity'
    INTERVENTIONS = 'interventions'
    OFFBOARD      = 'offboard'
    AGRO          = 'agro'


@unique
class Market(Enum):
    """
    https://iss.moex.com/iss/index
    https://iss.moex.com/iss/index.xml
    https://iss.moex.com/iss/index.json
    """
    BONDS  = 'bonds'
    INDEX  = 'index'
    SHARES = 'shares'


@unique
class Board(IntEnum):
    TQBR = 1


@unique
class Symbol(IntEnum):
    """
    https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities
    https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities.xml
    https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities.json
    """
    AFKS  = 1
    AFLT  = 2
    AGRO  = 3
    AKRN  = 4
    ALRS  = 5
    BANE  = 6
    BANEP = 7
    BSPB  = 8
    BSPBP = 9
    CBOM  = 10
    CHMF  = 11
    DSKY  = 12
    FEES  = 13
    FIVE  = 14
    FIXP  = 15
    GAZP  = 16
    GCHE  = 17
    GLTR  = 18
    GMKN  = 19
    HHRU  = 20
    HYDR  = 21
    IRAO  = 22
    KMAZ  = 23
    LKOH  = 24
    LNTA  = 25
    LSRG  = 26
    MAGN  = 27
    MAIL  = 28
    MGNT  = 29
    MOEX  = 30
    MSNG  = 31
    MSRS  = 32
    MSTT  = 33
    MTLR  = 34
    MTLRP = 35
    MTSS  = 36
    MVID  = 37
    NKNC  = 38
    NLMK  = 39
    NMTP  = 40
    NVTK  = 41
    OGKB  = 42
    OZON  = 43
    PHOR  = 44
    PIKK  = 45
    PLZL  = 46
    POGR  = 47
    POLY  = 48
    QIWI  = 49
    RASP  = 50
    RNFT  = 51
    ROSN  = 52
    RSTI  = 53
    RTKM  = 54
    RTKMP = 55
    RUAL  = 56
    SBER  = 57
    SBERP = 58
    SFIN  = 59
    SNGS  = 60
    SNGSP = 61
    SVAV  = 62
    TATN  = 63
    TATNP = 64
    TCSG  = 65
    TRMK  = 66
    TRNFP = 67
    UPRO  = 68
    URKA  = 69
    UWGN  = 70
    VSMO  = 71
    VTBR  = 72
    YNDX  = 73


@unique
class SymbolNight(IntEnum):
    """ On June 22, 2020, the following shares are allowed to trade at the evening trading session:
    https://www.moex.com/n28495
    https://www.moex.com/n29024
    """
    AFLT  = Symbol.AFLT
    ALRS  = Symbol.ALRS
    CHMF  = Symbol.CHMF
    FEES  = Symbol.FEES
    GAZP  = Symbol.GAZP
    GMKN  = Symbol.GMKN
    HYDR  = Symbol.HYDR
    IRAO  = Symbol.IRAO
    LKOH  = Symbol.LKOH
    MGNT  = Symbol.MGNT
    MOEX  = Symbol.MOEX
    MTSS  = Symbol.MTSS
    NLMK  = Symbol.NLMK
    NVTK  = Symbol.NVTK
    PLZL  = Symbol.PLZL
    POLY  = Symbol.POLY
    ROSN  = Symbol.ROSN
    SBER  = Symbol.SBER
    SBERP = Symbol.SBERP
    SNGS  = Symbol.SNGS
    SNGSP = Symbol.SNGSP
    TATN  = Symbol.TATN
    TATNP = Symbol.TATNP
    VTBR  = Symbol.VTBR
    YNDX  = Symbol.YNDX


@unique
class SymbolBig(IntEnum):
    """
    Symbols (from SymbolNight) with history candles starting from 2011.
    """
    AFLT  = Symbol.AFLT
    HYDR  = Symbol.HYDR
    IRAO  = Symbol.IRAO
    LKOH  = Symbol.LKOH
    MGNT  = Symbol.MGNT
    MTSS  = Symbol.MTSS
    NVTK  = Symbol.NVTK
    SBER  = Symbol.SBER
    SBERP = Symbol.SBERP
    TATN  = Symbol.TATN
    TATNP = Symbol.TATNP


@unique
class SymbolSmall(IntEnum):
    """
    Symbols (from SymbolMoex minus SymbolNight) with history candles starting from 2011.
    """
    LSRG  = Symbol.LSRG
    MSNG  = Symbol.MSNG
    MSRS  = Symbol.MSRS
    MTLR  = Symbol.MTLR
    MTLRP = Symbol.MTLRP
    OGKB  = Symbol.OGKB
    PIKK  = Symbol.PIKK
    RTKM  = Symbol.RTKM
    RTKMP = Symbol.RTKMP


@unique
class SymbolDelta(IntEnum):
    """
    Symbols: SymbolBlue minus SymbolBig minus SymbolSmall
    """
    AFKS  = Symbol.AFKS
    ALRS  = Symbol.ALRS
    CHMF  = Symbol.CHMF
    FEES  = Symbol.FEES
    FIVE  = Symbol.FIVE
    GAZP  = Symbol.GAZP
    GMKN  = Symbol.GMKN
    MAIL  = Symbol.MAIL
    MOEX  = Symbol.MOEX
    NLMK  = Symbol.NLMK
    PLZL  = Symbol.PLZL
    POLY  = Symbol.POLY
    ROSN  = Symbol.ROSN
    SNGS  = Symbol.SNGS
    SNGSP = Symbol.SNGSP
    TRNFP = Symbol.TRNFP
    VTBR  = Symbol.VTBR
    YNDX  = Symbol.YNDX


@unique
class SymbolBlue(IntEnum):
    """ MOEXBC [June 18, 2021 ... April 24, 2009]
    https://www.moex.com/en/index/MOEXBC
    Excluded by manual: -URKA
    Included by manual (from SymbolNight): +AFLT, +TATNP
    Included by manual (from MOEXBC history): +AFKS, +MAIL, +RTKM, +RTKMP, +TRNFP
    """
    AFKS  = Symbol.AFKS
    AFLT  = Symbol.AFLT
    ALRS  = Symbol.ALRS
    CHMF  = Symbol.CHMF
    FEES  = Symbol.FEES
    FIVE  = Symbol.FIVE
    GAZP  = Symbol.GAZP
    GMKN  = Symbol.GMKN
    HYDR  = Symbol.HYDR
    IRAO  = Symbol.IRAO
    LKOH  = Symbol.LKOH
    MAIL  = Symbol.MAIL
    MGNT  = Symbol.MGNT
    MOEX  = Symbol.MOEX
    MTSS  = Symbol.MTSS
    NLMK  = Symbol.NLMK
    NVTK  = Symbol.NVTK
    PLZL  = Symbol.PLZL
    POLY  = Symbol.POLY
    ROSN  = Symbol.ROSN
    RTKM  = Symbol.RTKM
    RTKMP = Symbol.RTKMP
    SBER  = Symbol.SBER
    SBERP = Symbol.SBERP
    SNGS  = Symbol.SNGS
    SNGSP = Symbol.SNGSP
    TATN  = Symbol.TATN
    TATNP = Symbol.TATNP
    TRNFP = Symbol.TRNFP
    VTBR  = Symbol.VTBR
    YNDX  = Symbol.YNDX


@unique
class SymbolMoex(IntEnum):
    """ IMOEX [June 18, 2021 ... December 18, 2012]
    https://www.moex.com/en/index/IMOEX
    """
    AFKS  = Symbol.AFKS
    AFLT  = Symbol.AFLT
    AGRO  = Symbol.AGRO
    AKRN  = Symbol.AKRN
    ALRS  = Symbol.ALRS
    BANE  = Symbol.BANE
    BANEP = Symbol.BANEP
    CBOM  = Symbol.CBOM
    CHMF  = Symbol.CHMF
    DSKY  = Symbol.DSKY
    FEES  = Symbol.FEES
    FIVE  = Symbol.FIVE
    FIXP  = Symbol.FIXP
    GAZP  = Symbol.GAZP
    GCHE  = Symbol.GCHE
    GLTR  = Symbol.GLTR
    GMKN  = Symbol.GMKN
    HHRU  = Symbol.HHRU
    HYDR  = Symbol.HYDR
    IRAO  = Symbol.IRAO
    KMAZ  = Symbol.KMAZ
    LKOH  = Symbol.LKOH
    LNTA  = Symbol.LNTA
    LSRG  = Symbol.LSRG
    MAGN  = Symbol.MAGN
    MAIL  = Symbol.MAIL
    MGNT  = Symbol.MGNT
    MOEX  = Symbol.MOEX
    MSNG  = Symbol.MSNG
    MSRS  = Symbol.MSRS
    MSTT  = Symbol.MSTT
    MTLR  = Symbol.MTLR
    MTLRP = Symbol.MTLRP
    MTSS  = Symbol.MTSS
    MVID  = Symbol.MVID
    NKNC  = Symbol.NKNC
    NLMK  = Symbol.NLMK
    NMTP  = Symbol.NMTP
    NVTK  = Symbol.NVTK
    OGKB  = Symbol.OGKB
    OZON  = Symbol.OZON
    PHOR  = Symbol.PHOR
    PIKK  = Symbol.PIKK
    PLZL  = Symbol.PLZL
    POGR  = Symbol.POGR
    POLY  = Symbol.POLY
    QIWI  = Symbol.QIWI
    RASP  = Symbol.RASP
    RNFT  = Symbol.RNFT
    ROSN  = Symbol.ROSN
    RSTI  = Symbol.RSTI
    RTKM  = Symbol.RTKM
    RTKMP = Symbol.RTKMP
    RUAL  = Symbol.RUAL
    SBER  = Symbol.SBER
    SBERP = Symbol.SBERP
    SFIN  = Symbol.SFIN
    SNGS  = Symbol.SNGS
    SNGSP = Symbol.SNGSP
    SVAV  = Symbol.SVAV
    TATN  = Symbol.TATN
    TATNP = Symbol.TATNP
    TCSG  = Symbol.TCSG
    TRMK  = Symbol.TRMK
    TRNFP = Symbol.TRNFP
    UPRO  = Symbol.UPRO
    UWGN  = Symbol.UWGN
    VSMO  = Symbol.VSMO
    VTBR  = Symbol.VTBR
    YNDX  = Symbol.YNDX


def get_url(symbol, data_format=DataFormat.JSON, engine=Engine.STOCK, market=Market.SHARES, board=Board.TQBR):
    """ Example:
    https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities/SBER/candles
    https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities/SBER/candles.xml
    https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities/SBER/candles.json
    """
    url_template = 'https://iss.moex.com/iss/engines/{engine}/markets/{market}/boards/{board}/securities/{symbol}/candles.{data_format}'
    return url_template.format(
        symbol=symbol.name,
        data_format=data_format.value,
        engine=engine.value,
        market=market.value,
        board=board.name)


def get_data(symbol, from_date='2000-01-01', till_date='2030-12-31', data_interval=DataInterval.M1, cursor_start=0):
    """ Example:
    https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities/SBER/candles?from=1992-01-01&till=2021-12-31&interval=1&start=0
    https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities/SBER/candles.xml?from=1992-01-01&till=2021-12-31&interval=1&start=0
    https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities/SBER/candles.json?from=1992-01-01&till=2021-12-31&interval=1&start=0
    """
    url = get_url(symbol)
    print('URL: {url}'.format(url=url))
    params = {
        'from'    : from_date,
        'till'    : till_date,
        'interval': data_interval.value,
        'start'   : cursor_start
    }
    return requests.get(url=url, params=params)


def parse_datetime(datetime_string):
    """ Example: '2012-01-03 18:19:59'
    """
    return datetime.strptime(datetime_string, '%Y-%m-%d %H:%M:%S')


def print_data(symbol):
    resp = get_data(symbol)
    #data = resp.json()
    #print(json.dumps(data, indent=4))
    print(resp.text)
    json = resp.json()
    candles = json['candles']['data']
    last_candle = candles[-1]
    last_datetime_string = last_candle[7]
    last_datetime = parse_datetime(last_datetime_string)
    print('last_datetime: {}'.format(last_datetime))


def main():
    print('Get data from MOEX...')
    print_data(SymbolBig.SBER)
    print('Done.')


if __name__ == "__main__":
    main()


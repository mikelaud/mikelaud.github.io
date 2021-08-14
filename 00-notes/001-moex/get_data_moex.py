"""
MOEX data downloader based on:
    Programming interface to ISS
    - https://www.moex.com/a2193
    - http://iss.moex.com/iss/reference/
    - https://fs.moex.com/files/6523
    - https://fs.moex.com/files/6524
=============================================
Run program:
---------------------------------------------
python3 get_data_moex.py
=============================================
Git help:
---------------------------------------------
user.name
user.password
git config --global --edit
git config --global credential.helper store
git add get_data_moex.py
git commit -m "message"
git push
=============================================
"""

from datetime import datetime
from enum import Enum, IntEnum, unique
from pathlib import Path
import inspect
import json
import os
import requests


class Const(object):
    DATA_DIR = 'data_moex'


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

    def __str__(self):
        return self.name


@unique
class SymbolsGroup(IntEnum):
    ALL   = 0
    BIG   = 1
    SMALL = 2
    MIX   = 3
    NIGHT = 4
    BLUE  = 5
    MOEX  = 6

    def __str__(self):
        return self.name


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

    @classmethod
    def group(_):
        return SymbolsGroup.ALL

    @classmethod
    def group_name(clazz):
        return str(clazz.group())

    @classmethod
    def group_len(clazz):
        return len(clazz)

    @classmethod
    def group_list(clazz):
        return list(clazz)

    @classmethod
    def to_string(_, symbols):
        return '{symbols}; ({count})'.format(symbols=','.join([s.name for s in symbols]), count=len(symbols))

    def __str__(self):
        return self.name


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

    @classmethod
    def group(_):
        return SymbolsGroup.NIGHT

    @classmethod
    def group_name(clazz):
        return str(clazz.group())

    @classmethod
    def group_len(clazz):
        return len(clazz)

    @classmethod
    def group_list(clazz):
        return list(clazz)

    def __str__(self):
        return self.name


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

    @classmethod
    def group(_):
        return SymbolsGroup.BIG

    @classmethod
    def group_name(clazz):
        return str(clazz.group())

    @classmethod
    def group_len(clazz):
        return len(clazz)

    @classmethod
    def group_list(clazz):
        return list(clazz)

    def __str__(self):
        return self.name


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

    @classmethod
    def group(_):
        return SymbolsGroup.SMALL

    @classmethod
    def group_name(clazz):
        return str(clazz.group())

    @classmethod
    def group_len(clazz):
        return len(clazz)

    @classmethod
    def group_list(clazz):
        return list(clazz)

    def __str__(self):
        return self.name


@unique
class SymbolMix(IntEnum):
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

    @classmethod
    def group(_):
        return SymbolsGroup.MIX

    @classmethod
    def group_name(clazz):
        return str(clazz.group())

    @classmethod
    def group_len(clazz):
        return len(clazz)

    @classmethod
    def group_list(clazz):
        return list(clazz)

    def __str__(self):
        return self.name


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

    @classmethod
    def group(_):
        return SymbolsGroup.BLUE

    @classmethod
    def group_name(clazz):
        return str(clazz.group())

    @classmethod
    def group_len(clazz):
        return len(clazz)

    @classmethod
    def group_list(clazz):
        return list(clazz)

    def __str__(self):
        return self.name


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

    @classmethod
    def group(_):
        return SymbolsGroup.MOEX

    @classmethod
    def group_name(clazz):
        return str(clazz.group())

    @classmethod
    def group_len(clazz):
        return len(clazz)

    @classmethod
    def group_list(clazz):
        return list(clazz)

    def __str__(self):
        return self.name


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


def create_location_dirs(location):
    path = Path(location)
    if not path.exists():
        print('Create location: {location}'.format(location=path))
        path.mkdir(parents=True, exist_ok=True)


def download_data_symbol(symbol_location, symbol):
    print('Symbol location: {location}'.format(location=symbol_location))
    create_location_dirs(symbol_location)


def download_data_symbols(data_location, symbols):
    symbols_total = len(symbols)
    current_group_name = 'unknown'
    current_number_in_group = 0
    current_total_in_group = 0
    for index in range(symbols_total):
        symbol = symbols[index]
        symbol_name = symbol.name
        symbol_group_name = symbol.group_name()
        symbol_location = os.path.join(data_location, symbol_group_name, symbol_name)
        symbol_number = index + 1
        if current_group_name != symbol_group_name:
            current_group_name = symbol_group_name
            current_total_in_group = symbol.group_len()
            current_number_in_group = 1
        else:
            current_number_in_group += 1
        print('Download symbol: {symbol} ({number} of {total}) of group {group} ({number_in_group} of {total_in_group})'.format(
            symbol=symbol_name,
            number=symbol_number,
            total=symbols_total,
            group=symbol_group_name,
            number_in_group=current_number_in_group,
            total_in_group=current_total_in_group
        ))
        download_data_symbol(symbol_location, symbol)


def get_home_location():
    filename = inspect.getframeinfo(inspect.currentframe()).filename
    return os.path.dirname(os.path.abspath(filename))


def get_data_location():
    return os.path.join(get_home_location(), Const.DATA_DIR)


def main():
    print_data(SymbolBig.SBER)
    home_location = get_home_location()
    data_dir = Const.DATA_DIR
    data_location = get_data_location()
    symbols_big = SymbolBig.group_list()
    symbols_small = SymbolSmall.group_list()
    symbols_mix = SymbolMix.group_list()
    symbols = symbols_big + symbols_small + symbols_mix
    print('MOEX data downloader')
    print('Home location: {location}'.format(location=home_location))
    print('Data dir     : {data_dir}'.format(data_dir=data_dir))
    print('Data location: {location}'.format(location=data_location))
    print('Symbols BIG  : {symbols}'.format(symbols=Symbol.to_string(symbols_big)))
    print('Symbols SMALL: {symbols}'.format(symbols=Symbol.to_string(symbols_small)))
    print('Symbols MIX  : {symbols}'.format(symbols=Symbol.to_string(symbols_mix)))
    print('Symbols Total: {count}'.format(count=len(symbols)))
    download_data_symbols(data_location, symbols)
    print('Done.')


if __name__ == "__main__":
    main()


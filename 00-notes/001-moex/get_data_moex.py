from enum import Enum, auto
import requests


class DataFormat(Enum):
    CSV  = 'csv'
    HTML = 'html'
    JSON = 'json'
    XML  = 'xml'


class Symbol(Enum):
    SBER = auto()


def get_url(symbol, data_format=DataFormat.JSON):
    url_template = 'https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities/{symbol}/candles.{data_format}'
    return url_template.format(symbol=symbol.name, data_format=data_format.value)


def get_data(symbol):
    print('URL: {url}'.format(url=get_url(symbol)))


def main():
    print('Get data from MOEX...')
    get_data(Symbol.SBER)
    print('Done.')


if __name__ == "__main__":
    main()


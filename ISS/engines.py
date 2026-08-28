import pandas as pd
import polars as pl
import numpy as np
import requests as rq

from typing import Iterable, Sequence
from warnings import warn, filterwarnings
from urllib.parse import urlencode
from datetime import datetime, date, timedelta

from .base import QueryFunctionFactory, TickerFunctionFactory
from ._utils import *

filterwarnings('default')

__all__ = ['available_engines', 'engine_info', 'engine_zcyc',
           'candles', 'available_markets', 'market_info',
           'secstats', 'market_zcyc', 'market_orderbook_info',
           'res_intra']


#ENGINES
def available_engines(lang: str = 'en') -> pl.DataFrame:

    '''
    Corresponds to iss.moex.com/iss/reference/391
    '''

    return pl.read_csv(f'https://iss.moex.com/iss/engines.csv?lang={lang}',
                       skip_rows=2,
                       has_header=True,
                       encoding='cp1251',
                       separator=';')

def engine_info(engine: str, lang: str = 'en') -> dict[str, pl.DataFrame]:

    '''
    https://iss.moex.com/iss/reference/397
    '''

    check_connection()
    
    res = rq.get(f'https://iss.moex.com/iss/engines/{engine}.json?lang={lang}')
    res.raise_for_status()
   
    res = res.json()

    info = {}
    time_cols = ['start_time', 'stop_time']

    for k in res.keys():
        info[k] = pl.from_records(res[k]['data'], schema=res[k]['columns'])
        if set(time_cols) < set(info[k].columns):
            info[k] = info[k].with_columns(pl.col(*time_cols).str.to_time(format="%H:%M:%S"))


    return info

@QueryFunctionFactory(base_url='https://iss.moex.com/iss/engines/{}/zcyc.json?',
                      unrelated_args=['engine'],
                      to_format=['engine']
                      )
def engine_zcyc(engine:str,
                date: str,
                lang: str = 'en',
                ) -> dict[str, pl.DataFrame]:
    '''
    Zero-coupon yield curve
    Corresponds to https://iss.moex.com/iss/reference/417
    '''

    return locals()

@TickerFunctionFactory(base_url='https://iss.moex.com/iss/engines/{}/markets/{}/securities/{}/candles.csv?',
                       unrelated_args=['verbose', 'engine', 'market', 'out'],
                       to_format=['engine', 'market', 'tickers'])
def _candle_single_day(engine: str,
                       market: str,
                       tickers: str | Iterable[str],
                       st: date | Iterable[date],
                       end: date | Iterable[date],
                       interval: int | Iterable[int] = 10,
                       verbose: bool = True,
                       out: str = 'polars',
                       ) -> dict[str, pl.DataFrame | pd.DataFrame | pl.LazyFrame]:
    kwargs = locals()

    kwargs['from'] = kwargs['st']
    kwargs['till'] = kwargs['end']

    kwargs.pop('st', None)
    kwargs.pop('end', None)

    kwargs['engine'] = ens_tuple(kwargs['engine'])
    kwargs['market'] = ens_tuple(kwargs['market'])
    kwargs['tickers'] = ens_tuple(kwargs['tickers'])

    return kwargs


def candles(engine: str,
            market: str,
            tickers: str | Iterable[str],
            st: date | Iterable[date],
            end: date | Iterable[date],
            interval: int | Iterable[int] = 10,
            verbose: bool = True,
            out: str = 'polars',
            ) -> dict[str, pl.DataFrame | pd.DataFrame | pl.LazyFrame]:
    '''
    
    '''
    
    kwargs = locals()

    tickers = ens_tuple(tickers)
    st: tuple[date] = ens_tuple(st)
    end: tuple[date] = ens_tuple(end)

    dfs = {}
    for ticker, ticker_st, ticker_end in zip(tickers, st, end):
    
        days_between = (ticker_end - ticker_st).days

        date_range = [ticker_st + timedelta(days=i) for i in range(days_between)]

        for end_dt in date_range:
            dfs_dt = _candle_single_day(tickers=ticker,
                                        st=end_dt - timedelta(days=1),
                                        end=end_dt,
                                        engine=engine,
                                        market=market,
                                        interval=interval)
            if len(dfs) == 0:
                dfs = {k : [v] for k, v in dfs_dt.items()}
            else:
                for k in dfs.keys():
                    dfs[k].append(dfs_dt[k])

    target_schema = {'open' : pl.Float64,
                     'close' : pl.Float64,
                     'high' : pl.Float64,
                     'low' : pl.Float64,
                     'value' : pl.Float64,
                     'volume' : pl.Int64,
                     'begin' : pl.String,
                     'end' : pl.String}

    for ticker in tickers:
        dfs[ticker] = map(lambda x: x.cast(target_schema),
                          dfs[ticker])
        dfs[ticker] = pl.concat(dfs[ticker])
        
        match out:

            case 'polars':
                dfs[ticker] = dfs[ticker]

            case 'polars_lazy':
                dfs[ticker] = dfs[ticker].lazy()
    
            case 'pandas':
                dfs[ticker] = dfs[ticker].to_pandas()

    return dfs

#MARKETS

def available_markets(engine: str,
                      lang: str = 'en') -> pl.DataFrame:

    '''
    Corresponds to https://iss.moex.com/iss/reference/343 
    '''

    check_connection()

    return pl.read_csv(f'https://iss.moex.com/iss/engines/{engine}/markets.csv?lang={lang}',
                       skip_rows=2,
                       has_header=True,
                       encoding='cp1251',
                       separator=';')


@QueryFunctionFactory(base_url='https://iss.moex.com/iss/engines/{}/markets/{}.json?',
                      unrelated_args=('engine', 'market'),
                      to_format=('engine', 'market'))
def market_info(engine: str,
                market: str,
                lang: str = 'en'
                ) -> dict[str, pl.DataFrame]:

    '''
    Correponds to https://iss.moex.com/iss/reference/351
    '''

    return locals()

def secstats(engine: str,
             market: str,
             lang: str = 'en',
             trading_session: int = 1,
             securities: Iterable[str] = ('GAZP',),
             board_id: Sequence[str] = ('TQBR',)
             ) -> pl.DataFrame:

    '''
    Get the intermediate results for the trading day
    Corresponds to the method in docs https://iss.moex.com/iss/reference/403
    '''
   
    check_connection()

    if len(board_id) > 10:
        raise ValueError(f'Expected 10 or less values in board_id, got {len(board_id)}')

    url = f'https://iss.moex.com/iss/engines/{engine}/markets/{market}/secstats.csv?'
    url = url + urlencode({'tradingsession' : trading_session,
                           'lang' : lang,
                           'securities' : ','.join(securities),
                           'boardid' : ','.join(board_id)
                           })

    return pl.read_csv(url,
                       encoding='cp1251',
                       separator=';',
                       skip_rows=2,
                       has_header=True)    


@QueryFunctionFactory(base_url='https://iss.moex.com/iss/engines/{engine}/markets/zcyc.json?',
                      unrelated_args=('engine'),
                      to_format=('engine'))
def market_zcyc(engine: str,
                lang: str = 'en',
                frm: str = '2000-01-01',
                tll: str = '2100-01-01',
                start: int = 0,
                ) -> dict[str, pl.DataFrame]:

    '''
    Data deprecated since 2018-01-03
    Correponds to https://iss.moex.com/iss/reference/405
    '''

    kwargs = locals()
    kwargs['from'] = kwargs['frm']
    kwargs['till'] = kwargs['tll']
    kwargs.pop('from', None)
    kwargs.pop('tll', None)

    warn('This method is deprecated since 2018-01-03 in the original ISS API', DeprecationWarning)

    return kwargs


def market_orderbook_info(engine: str,
                          market: str,
                          lang: str = 'en'
                          ) -> dict[str, pl.DataFrame]:
    '''
    Gives info on the orderbook for the particular market
    Corresponds to https://iss.moex.com/iss/reference/411

    This one is special cause of the /ordebook at the end
    '''

    check_connection()

    url=f'https://iss.moex.com/iss/engines/{engine}/markets/{market}.json?lang={lang}/orderbook'
    
    res = rq.get(url)
    res.raise_for_status()
   
    res = res.json()

    info = {}

    for k in res.keys():
        info[k] = pl.from_records(res[k]['data'], schema=res[k]['columns'])

    return info


class Market(object):

    def __init__(self,
                 engine: str,
                 market: str
                 ) -> None:

        self.engine = engine
        self.market = market

    def __repr__(self) -> str:
        return f'{self.engine}/{self.market}'


class Engine(object):

    markets: list[Market]

    def __init__(self, engine: str) -> None:

        if engine not in available_engines().drop_nulls()['name']:
            raise ValueError('Engine not in the list of available engines')

        self.engine = engine

    def available_markets(self, lang: str) -> pl.DataFrame:
        return available_markets(self.engine, lang)


def res_intra(engine: str | Iterable[str],
              market: str | Iterable[str],
              secstats: int | Iterable[int]=0,
              trsession: int | Iterable[int]=0,
              securities: str | Sequence[str]='',
              boardid: str | Iterable[str]='',
              verbose: bool = True,
              lang: str = 'en'
              ) -> dict[str, pl.DataFrame]:

    '''

    Get the information on the intraday results, only for the fund market

    Inputs:

        market:[str, list - trading market, default is None

        secstats:[int, list - intraday results, can be int or iterable, can take 3 possible values:
            1 for the main session, 2 for the evening session, 3 for the general summary, default is None

        trsession:[int, list - session data filter, works identically to secstats in terms of values, 
            default is None

        sec:[str, list - securities to get the stats about
        
        boardid:[str, list - board id, can be string or iterable, default is None

        verbose:bool - verbosity parameter, default is True so that when you start the function you seem cool or smth, idk

    Outputs:
        dfs:[dict, pd.DataFrame] - data on the intraday results, if several engines/ markets are requested returns an array
            with key as the unique engine-market combination and the value as the pd.DataFrame with the info  

    '''


    check_connection()

    base_url = f'https://iss.moex.com/iss/engines/{engine}/markets/{market}/secstats.csv?' 
        

    sec_info: dict[str, pl.DataFrame] = {}
    idx = 0
    for idx in tqdm(range(0, len(securities), 10),
                    desc=f'Fething data',
                    disable=not verbose,
                    unit=f' {",".join(securities[idx:idx+10])}',
                    leave=False
                    ):

        url = base_url + urlencode({'tradingsession' : trsession,
                                    'secstats' : secstats,
                                    'boardid' : boardid,
                                    'securities' : ','.join(securities[idx : idx+10])
                                    })

        df = pl.read_csv(url,
                         encoding='cp1251',
                         has_header=True,
                         skip_rows=2,
                         separator=';'
                         )

        for sec in securities[idx : idx+10]:
            sec_info[sec] = df.filter(pl.col('SECID') == sec) 

    return sec_info

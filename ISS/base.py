import pandas as pd
import polars as pl
import numpy as np
import datetime as dt

import inspect

from tqdm import tqdm
from abc import ABC, abstractmethod
from io import BytesIO
from functools import wraps
from dataclasses import dataclass, make_dataclass, fields
from urllib.parse import urlencode
from typing import Callable, Iterable, Any

from _utils import *

'''

This is the basic commands that can't be attributed to any distinct branch of commands in the MOEX ISS API

'''

__all__ = ['list_securities', 'security_specs', 'indxs4secs',
           'agg_info', 'market_info', 'turnovers',
           'turnover_cols']


class AbstractFunctionFactory(ABC):

    def __init__(self,
                 base_url: str,
                 unrelated_args: Iterable[str],
                 to_format: Iterable[str]
                 ) -> None:

        self.base_url = base_url
        self.unrelated_args = unrelated_args
        self.to_format = to_format

    @abstractmethod
    def __call__(self,
                 func: Callable
                 ) -> Callable[..., Any]:
        ...


class TickerFunctionFactory(AbstractFunctionFactory):

    '''
    Decorator class to be used to create ticker functions 
    '''

    __slots__ = ['base_url', 'unrelated_args', 'to_format']
    
    def __init__(self,
                 base_url: str,
                 unrelated_args: Iterable[str] | None = None,
                 to_format: Iterable[str] = ['tickers']
                 ) -> None:

        '''
        base_url: str - a formated string (like 'Hello, {}!')
        '''

        if unrelated_args is None:
            unrelated_args = {'verbose', 'out'} 

        super().__init__(base_url, unrelated_args, to_format)


    def __call__(self,
                 func: Callable
                ) -> Callable[..., dict[str, pd.DataFrame | pl.DataFrame | pl.LazyFrame]]:


        @wraps(func) 
        def wrapper(*args: Any,
                    **kwargs: Any
                    ) -> dict[str, pd.DataFrame | pl.DataFrame | pl.LazyFrame]:

            kwargs = func(*args, **kwargs)

            new_kwargs = {k: ens_tuple(v) for k, v in kwargs.items() 
                          if k not in self.unrelated_args
                          }

            new_kwargs = ens_same_length(new_kwargs)

            for arg in self.unrelated_args:
                if arg in kwargs:
                    new_kwargs[arg] = kwargs[arg]
            
            kwargs = new_kwargs
            del new_kwargs

            #raise error if shit hits the fan
            check_connection()

            ticker_descs = {}
            
            for idx, ticker in tqdm(enumerate(kwargs['tickers']),
                                    desc='Processing tickers',
                                    disable=not kwargs['verbose'],
                                    leave=False
                                    ):
                
                base_url_i = self.base_url.format(*[kwargs[fm][idx] for fm in self.to_format])
                kwargs_i = {k : v[idx] for k, v in kwargs.items() 
                            if k not in self.unrelated_args and k != 'tickers'
                            }
                ticker_descs[ticker] = pl.read_csv(
                                                rf"{base_url_i}{urlencode(kwargs_i)}",
                                                encoding='cp1251',
                                                skip_rows=2,
                                                has_header=True,
                                                quote_char=None,
                                                separator=';'
                                                )

                if kwargs['out'] == 'polars_lazy':
                    ticker_descs[ticker] = ticker_descs[ticker].lazy()

                elif kwargs['out'] == 'pandas':
                    ticker_descs[ticker] = ticker_descs[ticker].to_pandas()

                elif kwargs['out'] != 'polars':
                    raise NotImplementedError

            return ticker_descs

        return wrapper 


class QueryFunctionFactory(AbstractFunctionFactory):

    '''
    Factory for queries with several outputs
    '''

    __slots__ = ['base_url', 'unrelated_args', 'to_format']

    def __init__(self,
                 base_url: str,
                 unrelated_args: Iterable[str],
                 to_format: Iterable[str]
                 ) -> None:
        '''
        Inputs:
            to_format: arguments to format into the base_url
        '''


        super().__init__(base_url, unrelated_args, to_format)

    def __call__(self,
                 func: Callable[..., dict[str, pl.DataFrame]]
                 ) -> Callable[..., dict[str, pl.DataFrame]]:

        @wraps(func)
        def wrapper(*args: Any,
                    **kwargs: Any
                    ) -> dict[str, pl.DataFrame]:

            kwargs = func(*args, **kwargs)

            check_connection()
        
            url = self.base_url.format(*[kwargs[arg] for arg in self.to_format]) +\
                    urlencode({k : v for k, v in kwargs if k not in self.unrelated_args})

            res = rq.get(url)
            res.raise_for_status()
           
            res = res.json()

            info: dict[str, pl.DataFrame] = {}

            for k in res.keys():
                info[k] = pl.from_records(res[k]['data'], schema=res[k]['columns'])

            return info

        return wrapper


class OffsetFunctionFactory(AbstractFunctionFactory):

    __slots__ = ['base_url', 'unrelated_args',
                 'to_format', 'trouble_cols',
                 'increment_arg', 'increment']

    def __init__(self,
                 base_url: str,
                 unrelated_args: Iterable[str],
                 to_format: Iterable[str] = tuple(),
                 trouble_cols: Iterable[str] = tuple(),
                 increment_arg : str = 'start',
                 increment: int | dt.timedelta = 100,
                 ignore_start: int = 0,
                 ignore_end: int | None = None
                 ) -> None:

        super().__init__(base_url, unrelated_args, to_format)
        self.trouble_cols = trouble_cols
        self.increment = increment
        self.increment_arg = increment_arg
        self.ignore_start = ignore_start
        self.ignore_end = ignore_end

    def __call__(self,
                 func: Callable[..., dict[str, pl.DataFrame]]
                 ) -> Callable[..., pl.DataFrame | pl.LazyFrame | pd.DataFrame]:

        @wraps(func)
        def wrapper(*args,
                    **kwargs
                    ) -> pl.DataFrame | pl.LazyFrame | pd.DataFrame:

            '''
            Any function wrapped must also pass the `total` argument, which is `end` - `start`. 
            It was done because these can of different dtype like datetime, so the handling of that
            is outsources to preprocessing inside the function
            '''

            #always expect function to returns locals() 
            kwargs = func(*args, **kwargs)
            
            base_query = self.base_url.format(*[kwargs[nm] for nm in self.to_format])

            dfs = []
           
            for i in tqdm(range(kwargs['total']),
                          desc='Fetching data',
                          disable= not kwargs['verbose']):


                query = base_query + urlencode({k : v for k, v in kwargs.items()
                                                if k not in self.unrelated_args and
                                                k not in self.to_format
                                                })


                res = rq.get(query, timeout=kwargs['timeout'])
                res.raise_for_status()

                df_tmp = pl.read_csv(source=BytesIO(res.content),
                                     encoding="cp1251",
                                     separator=";",
                                     has_header=True,
                                     quote_char=None,
                                     skip_rows=2,
                                     n_threads=1,
                                     ignore_errors=True)
               
                if bool(self.trouble_cols):
                    #force string on trouble cols
                    df_tmp = df_tmp.with_columns(pl.col(*self.trouble_cols).cast(pl.String, strict=True))
                #cleanup from nulls 
                df_tmp = df_tmp.filter(~pl.all_horizontal(pl.all().is_null()))
                
                #to drop the cursor info etc.
                df_tmp = df_tmp[self.ignore_start:-self.ignore_end, :]


                if len(df_tmp) == 0:
                    if kwargs['verbose']:
                        print('No more entries found, finishing early...')
                    break

                dfs.append(df_tmp)

                kwargs.update({self.increment_arg : kwargs[self.increment_arg] + self.increment})

            if len(dfs) == 0:
                return pl.DataFrame()

            #sometimes full columns will be nulls so we use 'vertical_relaxed'
            if kwargs['out'] == 'pandas':
                df = pl.concat(dfs, how='vertical_relaxed').to_pandas()
                
            elif kwargs['out'] == 'polars':
                df = pl.concat(dfs, how='vertical_relaxed')

            elif kwargs['out'] == 'polars_lazy':
                df = pl.concat(map(lambda x: x.lazy(), dfs), how='vertical_relaxed')
            
            else:
                raise NotImplementedError

            return df
               

        return wrapper

#iss/securities
@OffsetFunctionFactory(base_url='https://iss.moex.com/iss/securities.csv?',
                       unrelated_args={'out', 'verbose', 'timeout', 'total'},
                       to_format=(),
                       trouble_cols=('emitent_id', 'emitent_inn', 'emitent_okpo', 'regnumber'))
def list_securities(q: str,
                    engine: str = 'stock',
                    trading: bool = True,
                    market: str = 'shares',
                    group_by: str = '',
                    start: int = 0,
                    end: int | None = None,
                    group_by_filter: str='',
                    verbose: bool=True,
                    lang: str='en',
                    out: str = 'polars',
                    timeout: int = 5
                    ) -> pd.DataFrame | pl.DataFrame | pl.LazyFrame:

    total = 1_000_000 if end is None else end - start

    return locals()


#/iss/securities/[security]
@TickerFunctionFactory(base_url='https://iss.moex.com/iss/securities/{}.csv?')
def security_specs(tickers: str | Iterable[str],
                   primary_board: bool | Iterable[bool] = True,
                   start: int | Iterable[int] = 0,
                   lang: str | Iterable[str]= 'en',
                   verbose: bool = True,
                   out: str | Iterable[str] = 'polars'
                   ) -> dict[str, pl.DataFrame | pl.LazyFrame | pd.DataFrame]:

    '''

    Get the description of a single of multiple security(-ies). 
    Corresponds to the api call from docs: https://iss.moex.com/iss/reference/193

    Inputs:
        
        tickers:str | Iterable[str] - ticker(-s) of the security

        primary_board:bool | Iterable[bool] - show only the primary board info, default is True

        start:int | Iterable[int] - index of line to start from, default is 0 

        verbose:bool - verbosity, default is True

        lang: str | Iterable[str] - language of output, can be 'en' or 'ru', default is en

        out: str | Iterable[str] - output format for each ticker, can be 'polars', 'pandas' or
            'polars_lazy'

    Outputs:
        
        ticker_descs: dict[str, pl.DataFrame | pl.LazyFrame | pd.DataFrame] - python dictionary with
            keys as tickers and values as the dataframes with their descriptions

    '''

    return locals()

#/iss/securities/[security]/indices
@TickerFunctionFactory(base_url="https://iss.moex.com/iss/securities/{}/indices.csv?")
def indxs4secs(tickers: str | Iterable[str],
               only_actual: bool | Iterable[bool] = True,
               lang: str | Iterable[str] = 'en',
               verbose: bool = True,
               out: str | Iterable[str] = 'polars'
               ) -> dict[str, pd.DataFrame | pl.LazyFrame | pl.DataFrame]:

    '''

    Get the indices in which the given security(-ies) is(are) mentioned.
        Corresponds to the api call from docs: https://iss.moex.com/iss/reference/199

    Inputs:
        
        tickers:[str, list, np.ndarray] - ticker(-s) to consider

        only_actual:bool - flag to return only indices still in use, default is True

        verbose:bool - verbosity flag, default is False

    Outputs:
        
        ticker_data: dict[str, pd.DataFrame, pl.LazyFrame, pl.DataFrame] - python dictionary of
            structure ticker : ticker_data

    '''
    return locals()

#/iss/securities/[security]/aggregates
@TickerFunctionFactory(base_url='https://iss.moex.com/iss/securities/{}/aggregates.csv?')
def agg_info(tickers: str | Iterable[str],
             dates: str | Iterable[str] = ('2020-06-05',),
             lang: str | Iterable[str]= ('en',),
             verbose: bool = True,
             out: str | Iterable[str] = 'polars'
             ) -> dict[str, pd.DataFrame | pl.LazyFrame | pl.DataFrame]:
    '''

    Get aggregate info on one or multiple indices/securities.
    Corresponds to the api call from docs: https://iss.moex.com/iss/reference/201 

    Inputs:

        tickers: str | Iterable[str] - a single ticker or a list of tickers

        dates: str | Iterable[str] - a single or an Iterable of dates
            It's assumed that each date corresponds to the security/ stock of the same index.

        verbose: bool - verbosity toggle, default is False

        lang: str | Iterable[str] - language of output, can be 'en' or 'ru', default is en


    Outputs:
        
        ticker_data: dict[str, pd.DataFrame, pl.LazyFrame, pl.DataFrame] - dict with strings as 
            tickers and values as respective dataframes for each ticker
    '''
    return locals()

#/iss/index 
def market_info(is_traded: bool = True,
                hide_inactive: bool = True,
                lang: str = 'en'
                ) -> dict[str, pl.DataFrame]:

    '''

    Get general market info
    Correponds to https://iss.moex.com/iss/reference/543

    Inputs:
        is_traded:bool - flag to show only currently traded boardgroups, default is True
        
        hide_inactive:bool - hide inactive security groups, default is True
        
        verbose:bool - verbosity flag, default is False
        
        lang:str - language of output, can be 'en' or 'ru', default is en

    Outputs:
        dfs:dict - info about the market 

    '''

    check_connection()

    url=f'https://iss.moex.com/iss/index.json?lang={lang}&is_traded={is_traded}&hide_inactive={hide_inactive}'
    
    res = rq.get(url)
    res.raise_for_status()
   
    res = res.json()

    info = {}

    for k in res.keys():
        info[k] = pl.from_records(res[k]['data'], schema=res[k]['columns'])

    return info

@OffsetFunctionFactory(base_url='https://iss.moex.com/iss/turnovers.csv?',
                       unrelated_args={'verbose', 'out', 'end', 'total', 'timeout'},
                       increment_arg='date',
                       increment=dt.timedelta(days=1))
def _pre_turnovers(is_tonight_session: bool = True,
                   start: dt.datetime | None = None,
                   end: dt.datetime = dt.datetime.now(),
                   verbose: bool = False,
                   lang: str = 'en',
                   out: str = 'polars',
                   timeout: int = 5
                   ) -> pl.DataFrame | pl.LazyFrame | pd.DataFrame:

    date = end.date() - dt.timedelta(days=1) if start is None else start
    total = 1 if start is None else (end.date() - start).days + 1 
    del start
    del end
   
    return locals()

#/iss/moex/turnovers
def turnovers(is_tonight_session: bool = True,
              start: dt.datetime | None = None,
              end: dt.datetime = dt.datetime.now(),
              verbose: bool = False,
              lang: str = 'en',
              out: str = 'polars',
              timeout: int = 5
              ) -> pl.DataFrame | pl.LazyFrame | pd.DataFrame:

    '''
    
    Get turnovers for markets for a specific date or a range of dates

    Inputs:
        is_tonight_session:bool - show turnovers for the evening session

        dt_st:[str, datetime.datetime] - start date in the format 'Y-M-D' to get the data from,
                                            default is None
                         
        dt_end:[str, datetime.datetime] - end date, same format, default is 'today'

        verbose:bool-verbosity flag, default is False
        
        lang:str - language of output, can be 'en' or 'ru', default is en

        Note: here the data is extracted from day [dt_st] to day [dt_end], not vice versa!

    Outputs:
        dfs:dict - dictionary with all the values for dates, dates are keys and pd.DataFrames are values

    '''
    
    check_connection()

    start = end.date() - dt.timedelta(days=1) if start is None else start

    kwargs = locals()
    df = _pre_turnovers(**kwargs)
    df = df.filter(~pl.col('NAME').is_in(['turnoversprevdate', 'turnovers', 'TOTALS'])).unique()
    return df.with_columns(pl.col('UPDATETIME').str.to_datetime(strict=False)).filter(pl.col('UPDATETIME') >= start)

#/iss/moex/turnovers/columns
def turnover_cols(lang:str='en')->pl.DataFrame:
    '''
        
    Get turnover columns description

    Inputs:
        lang:str - language of output, can be 'en' or 'ru', default is en


    Outputs:
        df:pd.DataFrame - description of turnover columns in the selected language

    '''

    check_connection()
    return pl.read_csv(rf"https://iss.moex.com/iss/engines/stock/turnovers/columns.csv?lang={lang}",
                       encoding='cp1251',
                       sep=';', 
                       skip_rows=2, 
                       has_header=True
                       )


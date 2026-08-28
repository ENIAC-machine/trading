import pandas as pd
import polars as pl
import numpy as np
import datetime as dt
import asyncio
import httpx

from tqdm.asyncio import tqdm
from abc import ABC, abstractmethod
from io import BytesIO
from functools import wraps
from dataclasses import dataclass, make_dataclass, fields
from urllib.parse import urlencode
from typing import Callable, Iterable, Any

from ISS._utils import *
from ISS.base import AbstractFunctionFactory

class AsyncTickerFunctionFactory(AbstractFunctionFactory):

    '''
    Decorator class to be used to create async ticker functions 
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

        return None

    @staticmethod
    async def _fetch_ticker_info(url: str) -> pl.DataFrame:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
                None, #default ThreadPoolExecutor cause we want to run this in 1 thread
                lambda: pl.read_csv(url,
                                    encoding='cp1251',
                                    skip_rows=2,
                                    has_header=True,
                                    quote_char=None,
                                    separator=';'
                                   )
                )

    def __call__(self,
                 func: Callable
                 ) -> Callable[..., dict[str, pd.DataFrame | pl.DataFrame | pl.LazyFrame]]:
        
        @wraps(func)
        async def wrapper(*args: Any,
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

            tasks = []
            async with asyncio.TaskGroup() as tg:
                for idx, ticker in tqdm(enumerate(kwargs['tickers']),
                                        desc='Processing tickers',
                                        disable=not kwargs['verbose'],
                                        leave=False):

                    base_url_i = self.base_url.format(*[kwargs[fm][idx] for fm in self.to_format])
                    kwargs_i = {k : v[idx] for k, v in kwargs.items() 
                            if k not in self.unrelated_args and k != 'tickers'
                            }

                    url = rf"{base_url_i}{urlencode(kwargs_i)}"
                    task = tg.create_task(self._fetch_ticker_info(url))
                    tasks.append(task)

            ticker_descs = {}

            for ticker in kwargs['tickers']:

                if kwargs['out'] == 'polars_lazy':
                    ticker_descs[ticker] = task.result().lazy()

                elif kwargs['out'] == 'pandas':
                    ticker_descs[ticker] = task.result().to_pandas()

                elif kwargs['out'] != 'polars':
                    raise NotImplementedError

            return ticker_descs

        return wrapper


#/iss/securities/[security]
@AsyncTickerFunctionFactory(base_url='https://iss.moex.com/iss/securities/{}.csv?')
async def security_specs(tickers: str | Iterable[str],
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
@AsyncTickerFunctionFactory(base_url="https://iss.moex.com/iss/securities/{}/indices.csv?")
async def indxs4secs(tickers: str | Iterable[str],
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
@AsyncTickerFunctionFactory(base_url='https://iss.moex.com/iss/securities/{}/aggregates.csv?')
async def agg_info(tickers: str | Iterable[str],
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



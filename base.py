import pandas as pd
import numpy as np
import scipy as sp
import requests as rq
import datetime as dt

from urllib.parse import urlencode
from typing import Union, Iterable
from utils import *

'''

This is the basic commands that can't be attributed to any distinct branch of commands in the MOEX ISS API

'''


def list_securities(q:str='',
                    engine:str='',
                    trading:bool=True,
                    market:str='',
                    group_by:str='',
                    start:int=0,
                    end:int=1_000_000,
                    group_by_filter:str='',
                    verbose:bool=True,
                    lang:str='en',
                    out: str = 'polars'
                    )->pd.DataFrame:
    
    r'''
    
    Get all available securities given filters. Corresponds to the api call from docs: https://iss.moex.com/iss/reference/205

    Inputs:
        q:str - que to search the instrument(s) by , in case of several instruments divide ques by space, 
                ques of no less than 3 characters are accepted

        engine:str - engine to select the securities from 

        trading:bool - whether to give securities that are currently trading or not, default is True

        market:str - the market to fetch the securities from

        group_by:str - group the result by field, currently 'group' and 'type' are available
  
        start:int - starting index of the security list, default is 0

        end:int - ending index of the security list, default 1_000_000 
                  as we can't easily fetch the number of available securities for this function call,
                  but it's not far from the max so I'll keep it this way for now
        
        group_by_filter:str - filters to group by, same available arguments as in group_by
                              but the arguments in this parameter must be the same or less than in group_by

        verbose:bool - verbosity, default is True 

        lang:str - language of output, can be 'ru' or 'en', default is 'en'

        out:str - output format, can be 'polars', 'polars_lazy' or 'pandas', default is 'polars'

    Outputs:
        df:pd.DataFrame | pl.DataFrame | pl.LazyFrame - a DataFrame with all the data

    ''' 

    end = 1e6 if end < 0 else end
    dfs = []
    check_connection()

    try:
        for i in tqdm(np.arange((end-start) // 100), desc='Fetching ticker data', disable=not verbose):
            query = rf"https://iss.moex.com/iss/securities.csv?q={q}&lang={lang}&engine={engine}&is_trading={int(trading)}&market={market}&group_by={group_by}&start={int(start+i*100)}&group_by_filter={group_by_filter}&limit={np.minimum(100, end-start-i*100).astype(int)}"
            df_tmp = pl.read_csv(source=query,
                                 encoding="ANSI",
                                 separator=";",
                                 has_header=True,
                                 skip_rows=1,
                                 n_threads=1,
                                 ignore_errors=True)
            if len(df_tmp) == 0:
                break
            
            else:
                dfs.append(df_tmp)

    except:
        raise GETError

    finally:

        if out == 'pandas':
            df = pl.concat(df).to_pandas()
        
        elif out == 'polars':
            df = pl.concat(dfs)
    
        elif out == 'polars_lazy':
            df = pl.concat(map(lambda x: x.lazy(), dfs))
        
        else:
            raise NotImplementedError

        return df


base_ticker_cfg = {'tickers' : [(str, Iterable[str]), ['GAZP']],
                   'lang' : [(str, Iterable[str]), ['en']],
                   'verbose' : [(bool), True],
                   'out' : [(str, Iterable[str]), 'polars']
                   }

def ticker_func_factory(base_url: str, 
                        doc: str,
                        func_kwargs: dict[str, list[tuple[type], Any] ]
                        ) -> callable[Any, pd.DataFrame | pl.DataFrame | pl.LazyFrame]:

    '''
    
    Function factory that creates function that use ticker as an argument

    Inputs:
        
        base_url: str - a formated string (like 'Hello, {}!')
        
        doc: str - a docstring
        
        func_kwargs: dict[str, list[tuple[type], Any] ] - a dictionary with keys being the argument names, and values being the list of 2 elements-
            one is the tuple of types the argument can be and the other is a default value. Among the arguments the mandatory are:
            -verbose (verbosity flag)
            -out (output format)
            -tickers (ticker value(-s) )

    Outputs:
        callable[Any, Any]
    
    '''

    
    @prep_kwargs
    def ticker_func(**kwargs):

        for k, v in func_kwargs.items():
            if k in kwargs:
                assert isinstance(kwargs[k], func_kwargs[k][0]), f"Argument {k} of incorrect type, expected {func_kwargs[k][0]}, got {type(v)}"
            else:
                kwargs[k] = func_kwargs[k][1] #set the default value    
            

        check_connection()

        ticker_descs = {}
        
        try:
            for idx in tqdm(np.arange(tickers.shape[0]),
                            desc='Processing tickers',
                            disable=not kwargs['verbose'],
                            leave=False
                            ):
                
                base_url_i = base_url.format(kwargs['tickers'][idx])
                kwargs_i = {k : v[idx] for k, v in kwargs.items()}
                ticker_descs[tickers[idx]] = pl.read_csv(
                                                rf"{base_url_i}{url_encode(kwargs_i)}",
                                                encoding='ANSI',
                                                skip_rows=2,
                                                has_header=True,
                                                separator=';'
                                                )

                if kwargs['out'][idx] == 'polars_lazy':
                    ticker_descs[tickers[idx]] = ticker_descs[tickers[idx]].lazy()

                elif kwargs['out'][idx] == 'pandas':
                    ticker_descs[tickers[idx]] = ticker_descs[tickers[idx]].to_pandas()

                elif kwargs['out'][idx] != 'polars':
                    raise NotImplementedError

        except:
            raise

        finally:
            return ticker_descs

    ticker_func.__doc__ = doc

    return ticker_func


#193

doc_193 = r'''

Get the description of a single of multiple security(-ies). Corresponds to the api call from docs: https://iss.moex.com/iss/reference/193

Inputs:
    
    tickers:str | Iterable[str] - ticker(-s) of the security

    primary_board:bool | Iterable[bool] - show only the primary board info, default is True

    start:int | Iterable[int] - index of line to start from, default is 0 

    verbose:bool - verbosity, default is True

    lang: str | Iterable[str] - language of output, can be 'en' or 'ru', default is en

    out: str | Iterable[str] - output format for each ticker, can be 'polars', 'pandas' or 'polars_lazy'

Outputs:
    
    ticker_descs: dict[str, pl.DataFrame | pl.LazyFrame | pd.DataFrame] - python dictionary with keys as tickers and values as the dataframes with their descriptions

'''

cfg_193 = base_cfg.copy()
cfg_193['primary_board'] = [(bool, Iterable[bool]), True]
cfg_193['start'] = [(int, Iterable[int]), 0]

security_specs = ticker_func_factory(base_url='https://iss.moex.com/iss/securities/{}.csv?',
                                     doc=doc_193,
                                     func_kwargs=cfg_193)


doc=r'''

Get the indices in which the given security(-ies) is(are) mentioned. Corresponds to the api call from docs: https://iss.moex.com/iss/reference/199

Inputs:
    
    tickers:[str, list, np.ndarray] - ticker(-s) to consider

    only_actual:bool - flag to return only indices still in use, default is True

    verbose:bool - verbosity flag, default is False

Outputs:
    
    ticker_data: dict[str, pd.DataFrame, pl.LaxyFrame, pl.DataFrame] - python dictionary of structure ticker : ticker_data

'''

cfg_199 = base_cfg.copy()
cfg_199['only_actual'] = [(bool, Iterable[bool]), True]


indxs4secs = ticker_func_factory(base_url="https://iss.moex.com/iss/securities/{}/indices.csv?",
                                 doc=doc_199,
                                 func_kwargs=cfg_199
                                 )


doc_201 =r'''

Get aggregate info on one or multiple indices/securities. Corresponds to the api call from docs: https://iss.moex.com/iss/reference/201 

Inputs:

    tickers:str | Iterable[str] - a single ticker or a list of tickers (can be in the form of numpy array), a multidimensional array will be flattened

    dates:str | Iterable[str] - a single or an Iterable of dates, it's assumed that each date corresponds to the security/ stock of the same index.

    verbose:bool - verbosity toggle, default is False

    lang: str | Iterable[str] - language of output, can be 'en' or 'ru', default is en


Outputs:
    
    df:[pd.DataFrame, np.ndarray, dict] - numpy array with dataframes with data for each ticker, dataframes' indices in the array correspond to the ticker's indices in the array 

'''

cfg_201 = base_cfg.copy()
cfg_201['dates'] = [(str, Iterable[str]), '2020-06-05']

agg_info = ticker_func_factory(base_url='https://iss.moex.com/iss/securities/{ticker}/aggregates.csv?',
                               doc=doc_201,
                               func_kwargs=cfg_201
                               ) 


def market_info(is_traded: bool = True,
                hide_inactive: bool = True,
                verbose: bool = False,
                lang: str = 'en'
                ) -> dict[str, pl.DataFrame]:

    '''

    Get general market info

    Inputs:
        is_traded:bool - flag to show only currently traded boardgroups, default is True
        
        hide_inactive:bool - hide inactive security groups, default is True
        
        verbose:bool - verbosity flag, default is False
        
        lang:str - language of output, can be 'en' or 'ru', default is en

    Outputs:
        dfs:dict - info about the market 

    '''

    check_connection()

    df_gen = pl.read_json(rf'https://iss.moex.com/iss/index.json?lang={lang}&is_traded={int(is_traded)}&hide_inactive={int(hide_inactive)}')

    #So here we can't just read .csv from pd.read_csv cause It will be bad,
    #so I have to read json and interpret it
    dfs = read_json(df_gen) 

    return dfs


def turnovers(is_tonight_session: bool = True,
              dt_st: str | dt.datetime = '',
              dt_end: str | dt.datetime = 'today',
              verbose: bool = False,
              lang: str = 'en'
              ) -> dict[str, pl.DataFrame]:

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

    dt_end = pd.to_datetime(dt_end)

    dt_st = dt_end if not dt_st else pd.to_datetime(dt_st)

    days = (dt_end - dt_st).days + 1

    dfs = dict()
    try:
        for day in tqdm(np.arange(days), desc='Retrieving days', disable=not verbose):
            dfs[dt_st+dt.timedelta(days=int(day))] = pd.read_csv(fr'https://iss.moex.com/iss/turnovers.csv?lang={lang}&is_tonight_session={int(is_tonight_session)}&date={(dt_st + dt.timedelta(days=int(day))).strftime("%Y-%m-%d")}',\
                                                            encoding='ANSI', header=1, sep=';', nrows=9)

    except:
        raise GETError

    finally:
        return dfs 

def turnover_cols(lang:str='en')->pd.DataFrame:
    r'''
        
    Get turnover columns description

    Inputs:
        lang:str - language of output, can be 'en' or 'ru', default is en


    Outputs:
        df:pd.DataFrame - description of turnover columns in the selected language

    '''

    check_connection()
    return pl.read_csv(rf"https://iss.moex.com/iss/engines/stock/turnovers/columns.csv?lang={lang}", encoding='ANSI', sep=';', skip_rows=2, has_header=True)




    #TODO: Implement https://iss.moex.com/iss/reference/439. Make sure that all the values in all of the arrays are of the appropriate dtype, also finish the implementation. Also consider making all calls asynchronous for speedup (maybe leave that as a project for your students). Also implement unit-tests for the functions with clear examples and maybe consider dropping the class stuff/ adding the ability to call the funcs without the class. Also consider that for some functions 2d inputs are possible, account for that cause rn it isn't accounted for. Also account for the fact that in some functions' native API calls it's possible to pass several values for one arg (to speed up the data loading). Also consider aactually separating this file into utils.py and the rest and also to split the query parts for the code to be a little bit more readable (it will still not be)




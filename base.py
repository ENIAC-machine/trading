import pandas as pd
import numpy as np
import scipy as sp
import requests as rq
import datetime as dt
from typing import Union, List
from utils import *

'''

This is the basic command that can't be attributed to any distinct branch of commands in the MOEX ISS API

'''


class GETError(Exception):    
    '''

    Custom class for Errors that happen when communicating with the market's server  

    '''

    def __init__(self, request_data, message:str="Error while requesting data from the market.\nNote that sometimes the market drops the communication and you just need to run the function again")->None:
        super().__init__(message)
        self.response = request_data

    def get_desc(self)->None:
        print(f'Returned status code is {self.response.status_code}\nStatus code description:\n{self.response.reason}')


def list_securities(q:str='', engine:str='', trading:bool=True,
                    market:str='', group_by:str='', start:int=0, end:int=1_000_000, group_by_filter:str='',
                    verbose:bool=True, lang:str='en')->pd.DataFrame:
    
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

    Outputs:
        df:pd.DataFrame - a DataFrame with all the data

    ''' 

    end = 1e6 if end < 0 else end
    dfs = []
    check_connection()

    try:
        for i in tqdm(np.arange((end-start) // 100), desc='Fetching ticker data', disable=not verbose):
            query = rf"https://iss.moex.com/iss/securities.csv?q={q}&lang={lang}&engine={engine}&is_trading={int(trading)}&market={market}&group_by={group_by}&start={int(start+i*100)}&group_by_filter={group_by_filter}&limit={np.minimum(100, end-start-i*100).astype(int)}"
            df_tmp = pd.read_csv(query, encoding="ANSI", sep=";", header=1, on_bad_lines='skip')
            if len(df_tmp) == 0:
                break
            else:
                dfs.append(df_tmp)

    except:
        raise GETError

    finally:
        df = pd.concat(dfs).reset_index(drop=True)
        return df

#TODO: redo as an interpretation of the json file as this version doesn't capture the board specs of a stock
def security_specs(tickers:Union[str, list, np.ndarray, pd.Series]='', primary_board:Union[bool, List[bool], np.ndarray]=True,
                   start:Union[int, List[int], np.ndarray]=0,
                   num_boards:Union[int, List[int], np.ndarray]=100,
                   verbose:bool=True, lang:Union[str, List[str], np.ndarray]='en')->dict:
    
    r'''
   
    Get the description of a single of multiple security(-ies). Corresponds to the api call from docs: https://iss.moex.com/iss/reference/193

    Inputs:
        
        tickers:str - ticker(-s) of the security

        primary_board:bool - show only the primary board info, default is True

        start:int - index of line to start from, default is 0 

        num_boards: [int, List[int], np.ndarray] - number of boards to show, default is 100

        verbose:bool - verbosity, default is True

        lang:str - language of output, can be 'en' or 'ru', default is en

    Outputs:
        
        ticker_descs:[dict, pd.DataFrame]- python dictionary with keys as tickers and values as the dataframes with their descriptions
    
    '''

    check_connection()

    #Reminder to remove this abomination, use locals().items()
    tickers = ens_nparr(tickers)
    primary_board = ens_nparr(primary_board)
    start = ens_nparr(start)
    num_boards = ens_nparr(num_boards)
    lang = ens_nparr(lang)
    
    #This too, it pains me that I wrote this
    tickers, primary_board, start, num_boards, lang = ens_same_length({key : val for key, val in locals().items() if key != 'verbose'}, False).values()

    ticker_descs = {}
    try:
        for idx in tqdm(np.arange(tickers.shape[0]), desc='Fetching info about tickers', disable=not verbose):
            ticker_descs[tickers[idx]] = read_json(
                                            pd.read_json(
                                            rf"https://iss.moex.com/iss/securities/{tickers[idx]}.json?lang={lang[idx]}&primary_board={int(primary_board[idx])}&start={start[idx]}",
                                            encoding='ANSI'
                                                        )
                                                    )
    except:
        raise GETError

    finally:
        return ticker_descs

    
def indxs4secs(tickers:Union[str, list, np.ndarray]='', only_actual:bool=True, verbose:bool=False, lang:str='en')->dict:
    
    r'''
    
    Get the indices in which the given security(-ies) is(are) mentioned. Corresponds to the api call from docs: https://iss.moex.com/iss/reference/199

    Inputs:
        
        tickers:[str, list, np.ndarray] - ticker(-s) to consider

        only_actual:bool - flag to return only indices still in use, default is True

        verbose:bool - verbosity flag, default is False

    Outputs:
        
        ticker_data:dict - pythod dictionary of structure ticker : ticker_data

    '''

    ticker_data = {}
    check_connection()

    tickers = ens_nparr(tickers)
    try:
        for ticker in tqdm(tickers, desc="Fetching tickers", disable= not verbose):
            ticker_data[ticker] = pd.read_csv(fr"https://iss.moex.com/iss/securities/{ticker}/indices.csv?lang={lang}&only_actual={only_actual}",
                                              encoding='ANSI', sep=';', header=1, on_bad_lines='skip')

    except:
        raise GETError

    finally:
        return ticker_data

##TODO: remove this abomination, make it via locals().items
def agg_info(tickers:Union[str, list, pd.Series, np.ndarray]='', dates:Union[str, list, pd.Series, np.ndarray]=None,
             verbose:bool=False, lang:Union[str, List[str], np.ndarray]='en')->Union[pd.DataFrame, np.ndarray]:
    
    r'''
    
    Get aggregate info on one or multiple indices/securities. Corresponds to the api call from docs: https://iss.moex.com/iss/reference/201 

    Inputs:

        tickers:[str, list, np.ndarray] - a single ticker or a list of tickers (can be in the form of numpy array), a multidimensional array will be flattened

        dates:[str, list, np.ndarray] - a single or a list of dates, it's assumed that each date corresponds to the security/ stock of the same index.
                                        It's possible to pass a 2D array where each subarray is a list of dates to retirieve information for

        verbose:bool - verbosity toggle, default is False

        lang:str - language of output, can be 'en' or 'ru', default is en


    Outputs:
        
        df:[pd.DataFrame, np.ndarray, dict] - numpy array with dataframes with data for each ticker, dataframes' indices in the array correspond to the ticker's indices in the array 

    '''
    assert dates, "You forgot to enter a date"

    check_connection()
 
    dates = pd.DataFrame(ens_nparr(dates)).to_numpy()
    dates = dates.reshape(1, -1) if len(dates.shape) == 1 else dates
    lang = ens_nparr(lang)

    tickers = ens_nparr(tickers).reshape(-1)

    assert tickers.shape[0] == dates.shape[0], f"Number of passed tickers doesn't match the number of (sets of) dates. Got {tickers.shape[0]} tickers and {dates.shape[0]} dates"

    return {ticker : {date : pd.read_csv(rf"https://iss.moex.com/iss/securities/{ticker}/aggregates.csv?date={date}&lang={lang[idx]}",
                                                                                                            encoding='ANSI', sep=';', header=1, on_bad_lines='skip')
                                for date in dates[idx] if date} 
                        for idx, ticker in enumerate(tickers)}


def market_info(is_traded:bool=True, hide_inactive:bool=True,
                verbose:bool=False, lang:str='en')->dict:

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

    df_gen = pd.read_json(rf'https://iss.moex.com/iss/index.json?lang={lang}&is_traded={int(is_traded)}&hide_inactive={int(hide_inactive)}')

    #So here we can't just read .csv from pd.read_csv cause It will be bad,
    #so I have to read json and interpret it
    dfs = read_json(df_gen) 

    return dfs


def turnovers(is_tonight_session:bool=True,
              dt_st:Union[str, dt.datetime]='',
              dt_end:Union[str, dt.datetime]='today',
              verbose:bool=False, lang:str='en')->dict:

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
    return pd.read_csv(rf"https://iss.moex.com/iss/engines/stock/turnovers/columns.csv?lang={lang}", encoding='ANSI', sep=';', header=1)




    #TODO: Implement https://iss.moex.com/iss/reference/439. Make sure that all the values in all of the arrays are of the appropriate dtype, also finish the implementation. Also consider making all calls asynchronous for speedup (maybe leave that as a project for your students). Also implement unit-tests for the functions with clear examples and maybe consider dropping the class stuff/ adding the ability to call the funcs without the class. Also consider that for some functions 2d inputs are possible, account for that cause rn it isn't accounted for. Also account for the fact that in some functions' native API calls it's possible to pass several values for one arg (to speed up the data loading). Also consider aactually separating this file into utils.py and the rest and also to split the query parts for the code to be a little bit more readable (it will still not be)




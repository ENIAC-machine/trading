import numpy as np
import polars as pl
import requests as rq
import datetime as dt

from json import loads
from functools import wraps
from tqdm import tqdm

from typing import Any, Callable

class GETError(Exception):    
    '''

    Custom class for Errors that happen when communicating with the market's server  

    '''

    def __init__(self,
                 request_data,
                 message:str = "Error while requesting data from the market.\nNote that sometimes the market drops the communication and you just need to run the function again"
                 )->None:
        super().__init__(message)
        self.response = request_data

    def get_desc(self)->None:
        print(f'Returned status code is {self.response.status_code}\nStatus code description:\n{self.response.reason}')



''' Check conenction by pinging https://iss.moex.com/iss/reference/ and getting code 200 '''
check_connection = lambda : None if rq.get(r'https://iss.moex.com/iss/reference/').status_code == 200 else exec(r"raise GETError(rq.get(r'https://iss.moex.com/iss/reference/'))")  

''' Ensure that the data is np.ndarray '''
ens_nparr = lambda arr: np.array([arr]) if type(arr) not in {np.ndarray, list} else np.array(arr)

''' Ensure data is of datetime type and correct format '''
ens_datetime = lambda val, fmt: dt.datetime.strptime(val, fmt) if type(val) in {str, np.str_} else val if type(val) == dt.datetime else exec("""raise ValueError(f"Can't convert, expected str datatype, got {type(val)}")""")


def ens_same_length(args:dict, verbose:bool=False)->dict:
    '''
    Ensures that all arguments' data is of the same length 
    

    Inputs:
        args:dict - dictionary of arguments of the function, where each key is the arg name and the value is the 
            arg list/np.ndarray of some length

        verbose:bool - verbosity param, default is False

    Outputs:
        args:dict - modified args dict with equal lengths across all passed arguments

    '''

    max_len = max(list(map(lambda x: len(ens_nparr(x)), args.values())))

    for key, arr in tqdm(args.items(), desc='Ensuring arguments', disable=not verbose):
        arr = ens_nparr(arr) if len(str(arr)) != 0 else np.array(['']) 
        if arr.shape[0] < max_len:
            args[key] = np.concatenate((arr, np.tile(arr[-1], max_len-arr.shape[0])))

    return args 


def prep_kwargs(func: Callable[..., Any]) -> Callable[..., Any]:

    '''
    Decorator to get proper form of the arguments of all basic functions
    (where the kwargs are only) supposed to contain `verbose` kwarg beside
    the arguments passed to the query
    '''


    @wraps(func)
    def wrapper(**kwargs):
        
        new_kwargs = ens_same_length({k: ens_nparr(v) for k, v in kwargs.items() if k != 'verbose'})

        new_kwargs['verbose'] = kwargs['verbose']

        return func(**new_kwargs)  


    return wrapper

def read_json(link:str
              ) -> dict[str, str]:
    '''

    Read json and convert it into a dictionary of dataframes

    Inputs:
        link:str - hyperlink

    Outputs:
        cnt:dict[str, str] - dictionary of dataframes 
    '''

    response = rq.get(link)
    response.encoding = 'ANSI'
    response.raise_for_status()

    cnt = loads(response.text.encode('ANSI'))

    #sometimes they have a .json with ONE key named 'contents', which is literally useless
    while True:
        keys = list(cnt.keys())
        if len(keys) == 1:
            cnt = cnt[keys[0]]
        else:
            break

    return cnt

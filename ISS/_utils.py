import numpy as np
import polars as pl
import requests as rq
import datetime as dt

from json import loads
from functools import wraps
from inspect import signature
from tqdm import tqdm

from typing import Any, Callable, Iterable

class GETError(Exception):    
    '''

    Custom class for Errors that happen when communicating with the market's server  

    '''

    def __init__(self,
                 request_data,
                 message: str = "Error while requesting data from the market.\nNote that sometimes the market drops the communication and you just need to run the function again"
                 )->None:

        super().__init__(message)
        self.response = request_data
        

    def get_desc(self)->None:
        print(f'Returned status code is {self.response.status_code}\nStatus code description:\n{self.response.reason}')


''' Check conenction by pinging https://iss.moex.com/iss/reference/ and getting code 200 '''
check_connection = lambda : None if rq.get(r'https://iss.moex.com/iss/reference/').status_code == 200 else rq.get(r'https://iss.moex.com/iss/reference/').raise_for_status()  

''' Ensure that the data is np.ndarray '''
ens_tuple = lambda arr: tuple([arr]) if not isinstance(arr, Iterable) or isinstance(arr, str) else tuple(arr)

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

    max_len = max(map(lambda x: len(ens_tuple(x)),
                      args.values()
                      )
                  )

    for key, arr in tqdm(args.items(),
                         desc='Ensuring arguments',
                         disable=not verbose):
        arr: tuple = ens_tuple(arr)
        if len(arr) < max_len:
            args[key] = arr + arr[-1]*max_len-len(arr)

    return args 


def prep_kwargs(unrelated_args: str | Iterable[str] | None = None
                ) -> Callable[..., Any]:

    '''
    Decorator to get proper form of the arguments of all basic functions
    (where the kwargs are only) supposed to contain `verbose` kwarg beside
    the arguments passed to the query
    '''

    if unrelated_args is None:
        unrelated_args = {}
    else:
        unrelated_args = set(unrelated_args)

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:

        sig = signature(func)

        @wraps(func)
        def wrapper(*args, **kwargs):

            kwargs = sig.bind(*args, **kwargs)
            kwargs.apply_defaults()
            kwargs = kwargs.arguments

            new_kwargs = {k: ens_tuple(v) for k, v in kwargs.items() 
                          if k not in unrelated_args
                          }
           
            new_kwargs = ens_same_length(new_kwargs)

            for arg in unrelated_args:
                if arg in kwargs:
                    new_kwargs[arg] = kwargs[arg]

            return func(**new_kwargs)  
        return wrapper
    return decorator

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

import numpy as np
import pandas as pd
import requests as rq
import datetime as dt
from tqdm import tqdm


''' Check conenction by pinging https://iss.moex.com/iss/reference/ and getting code 200 '''
check_connection = lambda : None if rq.get(r'https://iss.moex.com/iss/reference/').status_code == 200 else exec(r"raise GETError(rq.get(r'https://iss.moex.com/iss/reference/'))")  

''' Ensure that the data is np.ndarray '''
ens_nparr = lambda arr: np.array([arr]) if type(arr) not in {np.ndarray, list} else np.array(arr)

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

def read_json(json_file:pd.DataFrame=None, verbose:bool=False)->dict:
    r'''

    Read json and convert it into a dictionary of dataframes

    Inputs:
        json_file:pd.DataFrame - the json file that's (supposedly) read via pd.read_json function

        verbose:bool - verbosity flag, default is False

    Outputs:
        dfs:dict - dictionary of dataframes 
    '''

    dfs = {
            name : pd.DataFrame(json_file[name].iloc[2], columns=json_file[name].iloc[1])\
                for name in tqdm(json_file.columns, desc='Converting data into dataframes', disable= not verbose)
            }

    return dfs

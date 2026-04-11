import pandas as pd
import numpy as np
import scipy as sp
import requests as rq
import datetime as dt
from typing import Union, List
from .utils import *
from .base import GETError


##TODO: you can't read the .csv file, redo to interpret data from json and convert into .csv
def res_intra(engine:Union[str, List[str], np.ndarray]='', market:Union[str, List[str], np.ndarray]='',
              secstats:Union[int, List[int], np.ndarray]='', trsession:Union[int, List[int], np.ndarray]='',
              sec:Union[str, List[str], np.ndarray]='', boardid:Union[str, List[str], np.ndarray]='',
              verbose:bool=True, lang:str='en')->Union[pd.DataFrame, dict]:

    r'''

    Get the information on the intraday results, only for the fund market

    Inputs:

        market:[str, list, np.ndarray] - trading market, default is None

        secstats:[int, list, np.ndarray] - intraday results, can be int or iterable, can take 3 possible values:
            1 for the main session, 2 for the evening session, 3 for the general summary, default is None

        trsession:[int, list, np.ndarray] - session data filter, works identically to secstats in terms of values, 
            default is None

        sec:[str, list, np.ndarray] - securities to get the stats about
        
        boardid:[str, list, np.ndarray] - board id, can be string or iterable, default is None

        verbose:bool - verbosity parameter, default is True so that when you start the function you seem cool or smth, idk

    Outputs:
        dfs:[dict, pd.DataFrame] - data on the intraday results, if several engines/ markets are requested returns an array
            with key as the unique engine-market combination and the value as the pd.DataFrame with the info  

    '''

    check_connection()
    print(ens_same_length(locals()))
    assert None not in ens_same_length(locals()), rf"You didn't specify some of the arguments: {[nm for nm, arg in locals().items() if arg is None or None in arg]}"

    args = {nm : ens_nparr(arg) for nm, arg in locals().items() if nm != 'verbose'}
    args['secstats'] = np.array(list(map(lambda x: str(x)+'.json', args['secstats'])))
    
    assert max(map(lambda x: x.shape[0], args.values())) <= args['engine'].shape[0], "More arguments than markets considered"

    assert len(args['sec'].shape) <= 2, f"Too many dimensions for the securities array, expected no more than 2, got {len(args['sec'].shape)}"
    
    args = ens_same_length(args=args)

    dfs = []
    for idx in tqdm(np.arange(args['sec'].shape[0]), desc=f'Fething data', disable=not verbose):
        unique_secs = np.array(np.unique(args['sec'][idx]))
        batch = []
        for idx_batch in np.arange(int(np.ceil(unique_secs.shape[0] / 10))):
            batch += [read_json(pd.read_json(fr'https://iss.moex.com/iss/engines/stock/markets/{args["market"][idx]}/secstats={args["secstats"][idx]}?securities={",".join(unique_secs[idx_batch*10:idx_batch*10+10])}&tradingsession={args["trsession"][idx]}&boardid={",".join(args["boardid"][idx_batch*10:idx_batch*10+10])}&lang={lang}'))]
        
        dfs.append(batch)

    return dfs

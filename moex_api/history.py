import pandas as pd
import numpy as np
import scipy as sp
import requests as rq
import datetime as dt
from typing import Union, List
from .utils import *
from .base import GETError, agg_info


#TODO: add assert statements for argument debugging

def history(sec: Union[str, list, np.ndarray],
            engine: Union[str, list, np.ndarray]='stock',
            market: Union[str, list, np.ndarray]='shares',
            order: Union[str, list, np.ndarray]='asc',
            st: Union[str, dt.datetime, List[dt.datetime], List[str]]='2014-01-01', 
            end: Union[str, dt.datetime, List[dt.datetime], List[str]]='2037-12-31',
            numtrades:Union[int, list, np.ndarray]=0, 
            scol: Union[str, list, np.ndarray]='TRADEDATE',
            stline: Union[int, list, np.ndarray]=0, trsession:Union[int, str, list, np.ndarray]='', 
            marketpricebd: Union[bool, int, list, np.ndarray]=True,
            verbose: bool=False,
            lang: str='en',
            out: str='df' 
            )->Union[dict, pd.DataFrame, None]:

        '''
        
        Gives history on a security or a number of securities for a given engine and market for a given interval.
        All variables are defaulted for a case of a single security but they can be extrapolated for the case of 
        many tickers

        Inputs:
            sec:[str, list, np.ndarray] - a security or a list of securities to retrieve info about

            engine:[str, list, np.ndarray] - trading engine, default is 'stock'

            market:[str, list, np.ndarray] - trading market, default is 'shares'

            order:[str, list, np.ndarray] - sorting order, can take 2 values: 'asc' and 'desc', default is 'asc'

            st:[str, dt.datetime] - starting date to retrieve info from. Format is %Y-%m-%d, default is 2014-01-01

            end:[str, dt.datetime] - ending date to retrieve info from. Format is %Y-%m-%d, default is 2037-12-31

            numtrades:[int, list, np.ndarray] - minimum number of trades with the security(-ies)

            scol:[str, list, np.ndarray] - the column, by which to sort the data, default is 'TRADEDATE'

            stline:[int, list, np.ndarray] - the line to start from in the data output

            trsession:[int, str, list, np.ndarray] - the trading session(-s) to find the security(-ies) in. 
                Possible values are 0,1,2,3 or 'morning', 'main', 'evening', 'general' respectively. 
                Default is empty string

            marketpricebd:[bool, int, list, np.ndarray] - flag to give data only for the main mode of 
                trading for the security. Works only for engine='stock' and 
                market in ['shares', 'bonds', 'foreignshares'], default is True

            verbose:bool - verbosity flag, default is False

            lang:str - language of output, can be 'en' or 'ru', default is en
            
            out: str - what to return, can be 'df' to return multiindex dataFrame or 'dict' to return the
                        dictionary of the form {<name> : <dataframe>}, default value is 'df'

        Outputs:

            dfs:[dict, pd.DataFrame] - output data, if only one security is required, then outputs pd.DataFrame,
                otherwise outputs a dictionary where the stock ticker is key and the data on it is the value

        '''

        check_connection()

        try:
            args = {nm : ens_nparr(arg) if arg is not None else np.array([None]) 
                                                            for nm, arg in locals().items() if nm != 'verbose'}
        except:
            raise ValueError('Values in your array should all be of the same type')
        
        assert max(list(map(lambda x: x.shape[0], args.values()))) <= args['sec'].shape[0],\
                fr'Too many characteristics passed, expected {args["sec"].shape}, got {max(list(map(lambda x: x.shape, args.values())))}'

        #Ensure all arrays are of the same size
        args = ens_same_length(args=args)
        args['st'], args['end'] = ens_nparr(list(map(lambda x: ens_datetime(x, "%Y-%m-%d"), args['st']))), ens_nparr(list(map(lambda x: ens_datetime(x, "%Y-%m-%d"), args['end'])))

        dfs = dict()
        for i in tqdm(np.arange(args['sec'].shape[0]), desc='Getting data on securities', disable= not verbose):
            dfs_i = []
            num_days = (args['end'][i] - args['st'][i]).days
            iters = np.ceil(num_days / 100).astype(int)
            for j in tqdm(range(iters), desc=f'Fetching data on {args["sec"][i]}', leave=False, disable= not verbose):
                query = rf''' https://iss.moex.com/iss/history/engines/{args['engine'][i]}/markets/{args['market'][i]}/securities/{args['sec'][i]}.csv?sort_order={args['order'][i]}&from={str(args['st'][i]).split(' ')[0]}&till={str(args['end'][i]).split(' ')[0]}&numtrades={args['numtrades'][i]}&lang={lang}&limit={min(num_days-j*100, 100)}&sort_column={args['scol'][i]}&start={args['stline'][i]+100*j}&tradingsession={args['trsession'][i]}&marketprice_board={int(args['marketpricebd'][i])}'''
                #print(query)
                df_tmp = pd.read_csv(query, encoding='cp1251', sep=';', header=1, on_bad_lines='skip')
                if j == 0:
                    max_lines = df_tmp.iloc[-1, 1]
                    #print(iters)
                    iters = np.minimum(iters, np.ceil(float(max_lines) / 100).astype(int))
                #print(max_lines, iters)
                dfs_i.append(df_tmp.iloc[:-3])
            dfs[args['sec'][i]] = pd.concat(dfs_i, axis=0).reset_index(drop=True)
            dfs[args['sec'][i]].columns = pd.MultiIndex.from_tuples([(args['sec'][i], col) for col in 
                                                                                    dfs[args['sec'][i]].columns
                                                                     ],
                                                                    names=['DataFrame', 'Column']
                                                                    )

        if out == 'dict':
            return dfs if len(dfs.keys()) > 1 else list(dfs.values())[0]
        elif out == 'df':
            return pd.concat(list(dfs.values()), axis=1)
        else:
            raise NotImplementedError('This format is not implemented')


def trading_listing(engine:Union[str, List[str], np.ndarray]='stock', market:Union[str, List[str], np.ndarray]='shares',
                    status:Union[str, List[str], np.ndarray]='all', lang:Union[str, List[str], np.ndarray]='en', 
                    idx_st:Union[int, List[int], np.ndarray]=0, verbose:bool=True)->Union[dict, pd.DataFrame]:

    '''

    Get the list of traded/not-traded instruments. 
    IMOEX ISS reference: https://iss.moex.com/iss/reference/489

    Inputs:
        engine:[str, List[str], np.ndarray] - target engine(-s), default is stock

        market:[str, List[str], np.ndarray] - target market(-s), default is shares

        status:[str, List[str], np.ndarray] - status of the group of securities you want to fetch. Can take values 'traded', 'not traded', 'all', default is 'all'

        lang:[str, List[str], np.ndarray] - language of the output, can be 'en' or 'ru', default is 'en'

        idx_st:[int, List[int], np.ndarray] - index of the start of the output dataframe, default is 0

        verbose:bool - verbosity toggle, default is False

    Output:
        data:[dict, pd.DataFrame] - pandas dataframe with all of the data if only one of each value was given, otherwise dictionary where keys are engine|market
                                    and values are data on the respective engine+market combination
    
    '''


    check_connection()

    assert status in {'traded', 'not traded', 'all'}, f"Wrong input to the status argument, expected 'traded', 'not traded' or 'all', got {status}"

    assert lang in {'en', 'ru'}, f"Wrong input to the lang argument, expected 'en' or 'ru', got {lang}"

    args = ens_same_length({nm : ens_nparr(val) for nm, val in locals().items() if nm != 'verbose'})

    data = {}
    for idx in tqdm(np.arange(args['engine'].shape[0]), desc='Fetching data', disable=not verbose):
        dfs, iteration = [], 0
        print('\n')
        while True:
            print(f"\rDownloading data for engine {args['engine'][idx]} and market {args['market'][idx]}, currently downloaded {len(dfs)*100}", end='')
            dfs.append(pd.read_csv(fr"https://iss.moex.com/iss/history/engines/{args['engine'][idx]}/markets/{args['market'][idx]}/listing.csv?start={args['idx_st'][idx]+iteration*100}&status={args['status'][idx]}&lang={args['lang'][idx]}", encoding='cp1251', header=1, sep=';', on_bad_lines='skip'))
            iteration += 1
            if dfs[-1].shape[0] == 0:
                break
            #else:
                #print(dfs[-1].shape[0])
        data['|'.join([args['engine'][idx], args['market'][idx]])] = pd.concat(dfs).reset_index(drop=True)

    return data if len(data.keys()) > 1 else list(data.values())[0]

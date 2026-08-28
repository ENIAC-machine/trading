import polars as pl
import numpy as np
import datetime as dt

from urllib.parse import urlencode
from typing import Iterable

from base import OffsetFunctionFactory
from _utils import *

__all__ = ['history', 'trading_listing']


#439
@OffsetFunctionFactory(base_url='https://iss.moex.com/iss/history/engines/{}/markets/{}/securities/{}.csv?',
                       unrelated_args=('verbose', 'lang', 'timeout', 'out'),
                       to_format=('engine', 'market', 'security'),
                       increment_arg='start',
                       increment=100,
                       ignore_end=3)
def history(security: str ,
            engine: str = 'stock',
            market: str = 'shares',
            sort_order: str ='asc',
            st: dt.date = dt.date(2014, 1, 1), 
            end: dt.date = dt.date(2037, 12, 31),
            numtrades: int = 0, 
            tradingsession: str = '', 
            marketprice_board: bool = True,
            verbose: bool = False,
            lang: str = 'en',
            timeout: int = 5,
            out: str = 'polars'
            ) -> dict[str, pl.DataFrame | pl.LazyFrame]:

    total = np.ceil((end - st).days / 100).astype(int)

    kwargs = locals()
    kwargs['start'] = 0
    kwargs['limit'] = 100
    kwargs['from'] = kwargs['st']
    kwargs['till'] = kwargs['end']
    kwargs.pop('st')
    kwargs.pop('end')

    return kwargs


#TODO: use the OffsetFunctionFactory on this
#489
@prep_kwargs(unrelated_args=('lang', 'verbose'))
def trading_listing(engine: str | Iterable[str] = 'stock',
                    market: str | Iterable[str] ='shares',
                    status: str | Iterable[str] = 'all',
                    lang: str | Iterable[str] = 'en', 
                    idx_st: int | Iterable[int] = 0,
                    verbose: bool = True
                    )-> dict[str, pl.DataFrame]:

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

    if status not in {'traded', 'not traded', 'all'}:
        raise ValueError(f"Wrong input to the status argument, expected 'traded', 'not traded' or 'all', got {status}")

    args = locals()

    data: dict[str, pl.DataFrame] = {}

    for idx in tqdm(range(len(args['engine'])),
                    desc='Fetching data',
                    leave=False,
                    disable=not verbose):
        dfs, iteration = [], 0
        
        while True:
            dfs.append(pl.read_csv(fr"https://iss.moex.com/iss/history/engines/{args['engine'][idx]}/markets/{args['market'][idx]}/listing.csv?start={args['idx_st'][idx]+iteration*100}&status={args['status'][idx]}&lang={args['lang'][idx]}",
                                   encoding='cp1251',
                                   has_header=True,
                                   separator=';',
                                   skip_rows=2,
                                   ignore_errors=True
                                   ))
            iteration += 1
            if dfs[-1].shape[0] == 0:
                break
        
        data['|'.join([args['engine'][idx], args['market'][idx]])] = pl.concat(dfs)

    return data

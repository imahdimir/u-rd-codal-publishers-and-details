"""

    """

import asyncio
import time
from functools import partial
from pathlib import Path

import pandas as pd
from githubdata import GithubData
from mirutil import async_requests as areq
from mirutil import utils as mu
from mirutil.df_utils import read_data_according_to_type as read_data
from mirutil.df_utils import save_as_prq_wo_index as sprq
from persiantools.characters import fa_to_ar


class RepoUrl :
    targ = 'https://github.com/imahdimir/raw-d-codal_ir-nasheran'
    cur = 'https://github.com/imahdimir/u-raw-d-codal_ir-nasheran'
    src = 'https://github.com/imahdimir/d-all-Codal-letters'

ru = RepoUrl()

class ColName :
    ctic = 'CodalTicker'
    sym = 'Symbol'
    asym = 'ArSymbol'
    aurl = 'aurl'
    furl = 'furl'
    art = 'art'
    frt = 'frt'
    obsd = 'ObsDate'
    url = 'url'

cn = ColName()

class Constant :
    base_url = 'https://www.codal.ir/Company.aspx?Symbol='

cte = Constant()

def main() :
    pass

    ## get all codal letters data
    gd_src = GithubData(ru.src)
    dfs = gd_src.read_data()
    ##
    gd_src.rmdir()

    ## keep only codal tickers
    dfs = dfs[[cn.ctic]]
    dfs = dfs.drop_duplicates()
    dfs = dfs.dropna()
    ## relace space in codal tickers with +
    dfs[cn.sym] = dfs[cn.ctic].str.replace(' ' , '+')
    dfs[cn.asym] = dfs[cn.sym].apply(fa_to_ar)
    ## add leading base url
    dfs[cn.aurl] = cte.base_url + dfs[cn.asym]
    ##
    clus = mu.return_clusters_indices(dfs)
    fu = partial(areq.get_reps_texts_async)
    ##
    for se in clus :
        si = se[0]
        ei = se[1] + 1
        print(se)

        inds = dfs.iloc[si : ei].index

        aurls = dfs.loc[inds , cn.aurl]
        dfs.loc[inds , cn.art] = asyncio.run(fu(aurls))

        # break

    ##
    sprq(dfs , 't0.prq')

    ##
    dfs[cn.furl] = cte.base_url + dfs[cn.sym]
    cls = mu.return_clusters_indices(dfs)
    ##
    for se in cls :
        si = se[0]
        ei = se[1] + 1
        print(se)

        inds = dfs.iloc[si : ei].index

        aurls = dfs.loc[inds , cn.furl]
        dfs.loc[inds , cn.frt] = asyncio.run(fu(aurls))

        time.sleep(3)

        # break

    ##
    sprq(dfs , 't00.prq')

    ##
    msk = dfs[cn.frt].isna()
    df1 = dfs[msk]

    ##
    cls = mu.return_clusters_indices(df1)
    ##
    for se in cls :
        si = se[0]
        ei = se[1] + 1
        print(se)

        inds = df1.iloc[si : ei].index

        aurls = dfs.loc[inds , cn.furl]
        dfs.loc[inds , cn.frt] = asyncio.run(fu(aurls))

        time.sleep(3)

        # break

    ##
    msk = dfs[cn.frt].isna()
    df1 = dfs[msk]

    ##
    sprq(dfs , 't00.prq')
    ##
    dfs = read_data('t00.prq')

    ##
    dfa = pd.DataFrame()

    for _ , row in dfs.iterrows() :
        _d0 = pd.read_html(row[cn.art])[0]
        _d1 = pd.read_html(row[cn.frt])[0]

        _d0 = pd.concat([_d0 , _d1] , axis = 0)

        _d0[cn.ctic] = row[cn.ctic]
        _d0[cn.url] = row[cn.aurl]

        dfa = pd.concat([dfa , _d0] , axis = 0)

    ##
    dfa = dfa.drop_duplicates()

    ##
    dfa.columns = [str(x) for x in dfa.columns]
    sprq(dfa , 't10.prq')
    ##
    dfa = read_data('t10.prq')

    ##
    msk = dfs[cn.ctic].isin(dfa[cn.ctic])
    assert msk.all()

    ##
    dfa[cn.obsd] = pd.to_datetime('today').date()

    ##

    gd_targ = GithubData(ru.targ)
    gd_targ.overwriting_clone()
    ##
    dft = gd_targ.read_data()
    ##
    dft = pd.DataFrame()
    ##
    dft = pd.concat([dft , dfa])
    ##
    dft = dft.drop_duplicates()
    ##
    sprq(dft , gd_targ.data_fp)

    ##
    msg = 'updated by: '
    msg += ru.cur
    ##
    gd_targ.commit_and_push(msg)

    ##

    gd_src.rmdir()
    gd_targ.rmdir()

    ##
    _2del = {
            't0.prq'  : None ,
            't00.prq' : None ,
            't1.prq'  : None ,
            't10.prq' : None ,
            }

    for pn in _2del.keys() :
        Path(pn).unlink(missing_ok = True)

    ##


    ##

##
if __name__ == "__main__" :
    main()

##
# noinspection PyUnreachableCode
if False :

    pass

    ##


    ##


    ##

##

##

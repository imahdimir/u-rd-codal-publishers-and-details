"""

    """

##

from githubdata import GithubData
import pandas as pd
from persiantools.characters import fa_to_ar
from mirutil import async_requests as areq
from mirutil import utils as mu
from functools import partial
import asyncio
from mirutil.df_utils import save_as_prq_wo_index as sprq


class RepoUrl :
    targ = 'https://github.com/imahdimir/raw-d-codal_ir-nasheran'
    cur = 'https://github.com/imahdimir/u-d-firm-ISIC-in-codal'
    src = 'https://github.com/imahdimir/d-all-Codal-letters'

ru = RepoUrl()

class ColName :
    ctic = 'CodalTicker'
    sym = 'Symbol'
    asym = 'ArabicSymbol'
    aurl = 'aurl'
    obsd = 'ObsDate'
    url = 'url'

cn = ColName()

class Constant :
    base_url = 'https://www.codal.ir/Company.aspx?Symbol='

def main() :

    pass

    ##
    rp_src = GithubData(ru.src)
    dfs = rp_src.read_data()
    dfsv = dfs.head()
    ##
    dfs = dfs[[cn.ctic]]
    dfs = dfs.drop_duplicates()
    ##
    dfs = dfs.dropna()
    ##
    dfs[cn.sym] = dfs[cn.ctic].str.replace(' ' , '+')
    dfs[cn.asym] = dfs[cn.sym].apply(fa_to_ar)
    ##
    dfs[cn.aurl] = Constant.base_url + dfs[cn.asym]

    ##
    clus = mu.return_clusters_indices(dfs)
    ##
    fu = partial(areq.get_reps_texts_async)
    ##
    for se in clus :
        si = se[0]
        ei = se[1]
        print(se)

        inds = dfs.iloc[si : ei + 1].index

        urls = dfs.loc[inds , cn.aurl]

        dfs.loc[inds , 'rt'] = asyncio.run(fu(urls))

        # break

    ##
    dfa = pd.DataFrame()

    for _ , row in dfs.iterrows() :
        _df = pd.read_html(row['rt'])[0]
        _df[cn.ctic] = row[cn.ctic]
        _df[cn.url] = row[cn.aurl]

        dfa = pd.concat([dfa , _df])

    ##
    msk = dfs[cn.ctic].isin(dfa[cn.ctic])
    msk.all()

    ##
    dfa[cn.obsd] = pd.to_datetime('today').date()

    ##
    rp_targ = GithubData(ru.targ)
    rp_targ.clone()

    ##
    dfa.columns = [str(x) for x in dfa.columns]
    ##
    dft = rp_targ.read_data()
    ##
    dft = pd.concat([dft , dfa])
    ##
    dft = dft.drop_duplicates()
    ##
    sprq(dft , rp_targ.data_fp)

    ##
    tokp = '/Users/mahdi/Dropbox/tok.txt'
    tok = mu.get_tok_if_accessible(tokp)

    ##
    msg = 'init'
    rp_targ.commit_and_push(msg , user = rp_targ.user_name , token = tok)

    ##

    rp_src.rmdir()
    rp_targ.rmdir()


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
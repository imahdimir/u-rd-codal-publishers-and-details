"""

    """

import asyncio
import re
import time
from functools import partial

import pandas as pd
import requests
from bs4 import BeautifulSoup
from githubdata import GithubData
from mirutil.async_requests import get_reps_texts_async
from mirutil.df_utils import read_data_according_to_type as read_data
from mirutil.df_utils import save_as_prq_wo_index as sprq
from mirutil.utils import ret_clusters_indices as fu0


class GDUrl :
    trg0 = 'https://github.com/imahdimir/rd-codal-publishers'
    trg1 = 'https://github.com/imahdimir/rd-codal-publisher-details'
    cur = 'https://github.com/imahdimir/u-raw-d-codal_ir-nasheran'

gdu = GDUrl()

class ColName :
    pgn = 'pg_no'
    pgurl = 'pg_url'
    pgres = 'pg_res'
    namad = 'نماد'
    nurl = 'namad_url'
    nres = 'namda_res'
    merg = '4merge'
    obds = 'ObsDate'

c = ColName()

class Constant :
    base_url = 'https://my.codal.ir/fa/publishers/?page='
    itmes_per_page = 12

cte = Constant()

def find_number_of_pages() :
    res = requests.get(cte.base_url + '1')
    soup = BeautifulSoup(res.text , 'html.parser')
    ls = soup.find_all(class_ = 'text_record')
    assert len(ls) == 1
    ou = ls[0].text
    ou = int(re.sub("[^0-9]" , "" , ou))
    ou = ou // cte.itmes_per_page
    ou += 1
    return ou

def find_sherkat_urls(res) :
    soup = BeautifulSoup(res , 'html.parser')
    ls = soup.find_all('a' , href = True)
    ls = [x for x in ls if x['href'].startswith('/fa/publisher/')]
    df = pd.DataFrame()
    df[c.namad] = [x.text for x in ls]
    df[c.namad] = df[c.namad].str.strip()
    df[c.nurl] = [x['href'] for x in ls]
    return df

def main() :
    pass

    ##
    pgs_no = find_number_of_pages()
    pgs_no
    ##
    df = pd.DataFrame()
    df[c.pgn] = range(1 , pgs_no + 1)
    ##
    df[c.pgurl] = cte.base_url + df[c.pgn].astype(str)
    ##
    hdrs = {
            'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.114 Safari/537.36'
            }
    fu = partial(get_reps_texts_async , headers = hdrs)
    ##
    df[c.pgres] = None
    df1 = df.copy()
    ##
    while df1.shape[0] > 0 :
        msk = df[c.pgres].isna()
        df1 = df[msk]
        print(len(msk[msk]))

        clus = fu0(df1)

        for se in clus :
            print(se)
            inds = df1.iloc[se[0] : se[1]].index

            urls = df.loc[inds , c.pgurl]
            df.loc[inds , c.pgres] = asyncio.run(fu(urls))

            time.sleep(2)

            # break

    ##
    sprq(df , 't0.prq')
    ##
    df = read_data('t0.prq')
    ##
    dfa = pd.DataFrame()

    for _ , ro in df.iterrows() :
        _df = pd.read_html(ro[c.pgres])[0]
        _df[c.pgn] = ro[c.pgn]
        _df[c.pgurl] = ro[c.pgurl]
        dfa = pd.concat([dfa , _df])

    ##
    msk = dfa.duplicated(c.namad)
    df1 = dfa[msk]
    assert len(df1) == 1
    ##
    dfa = dfa.drop_duplicates(c.namad)
    ##
    dfa[c.merg] = dfa[c.namad].str.replace('\s' , '')
    assert dfa[c.merg].is_unique

    ##
    dfb = pd.DataFrame()

    for _ , ro in df.iterrows() :
        _df = find_sherkat_urls(ro[c.pgres])
        dfb = pd.concat([dfb , _df])

    ##
    msk = dfb.duplicated(c.namad)
    df1 = dfb[msk]
    assert len(df1) == 1
    ##
    dfb.drop_duplicates(subset = c.namad , inplace = True)
    ##
    dfb[c.merg] = dfb[c.namad].str.replace('\s' , '')
    assert dfb[c.merg].is_unique
    ##
    dfc = dfa.merge(dfb , on = c.merg)
    ##
    dfc[c.nurl] = 'https://my.codal.ir' + dfc[c.nurl]
    ##
    dfc[c.obds] = pd.to_datetime('today').date()
    ##

    gd_trg0 = GithubData(gdu.trg0)
    gd_trg0.overwriting_clone()
    ##
    d0p = gd_trg0.data_fp
    d0 = read_data(d0p)
    ##
    d0 = pd.concat([d0 , dfc])
    ##
    d0.drop_duplicates(inplace = True)
    ##
    sprq(d0 , d0p)
    ##
    msg = 'data updated by: '
    msg += gdu.cur
    ##
    gd_trg0.commit_and_push(msg)
    ##
    gd_trg0.rmdir()
    ##

    ##
    dfc[c.nres] = None
    df1 = df.copy()
    ##
    while df1.shape[0] > 0 :
        msk = dfc[c.nres].isna()
        df1 = dfc[msk]
        print(len(msk[msk]))

        clus = fu0(df1 , 20)

        for se in clus :
            print(se)
            inds = df1.iloc[se[0] : se[1]].index

            urls = dfc.loc[inds , c.nurl]
            dfc.loc[inds , c.nres] = asyncio.run(fu(urls))

            time.sleep(1)

            # break

        # break

    ##
    sprq(dfc , 't1.prq')
    ##
    dfc = read_data('t1.prq')
    ##
    dd = pd.DataFrame()

    for _ , ro in dfc.iterrows() :
        _df = pd.read_html(ro[c.nres])[0]
        _df[c.namad] = ro[c.namad + '_x']
        _df[c.nurl] = ro[c.nurl]
        dd = pd.concat([dd , _df])

    ##
    dd = dd.drop_duplicates()
    ##

    ##
    gd_trg1 = GithubData(gdu.trg1)
    gd_trg1.overwriting_clone()
    ##
    d1p = gd_trg1.data_fp
    ##
    dft = gd_trg1.read_data()
    ##
    dft = pd.concat([dft , dd])
    ##
    dft = dft.drop_duplicates()
    ##
    sprq(dft , d1p)
    ##
    msg = 'New Version by: '
    msg += gdu.cur
    ##
    gd_trg1.commit_and_push(msg)

    ##

    gd_trg1.rmdir()

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

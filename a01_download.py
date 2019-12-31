#%%
from ap.classes import Download_per_date
from ap.tools import *

DB = 'Analysis'
INIT = 'init'  

START_DATE = '20190102'
END_DATE = '20190103'

TB1 = 'a01_30017'
PREFIX1 = '30017'
QUERY_STR_PARAMS1 = {
    'name': 'fileDown',
    'filetype': 'xls',
    'url': 'MKD/04/0404/04040400/mkd04040400',
    'stctype': 'ALL',
    'var_invr_cd': '1000',
    'schdate': '',
    'etctype': 'ST',
    'pagePath': '/contents/MKD/04/0404/04040400/MKD04040400.jsp'
}

TB2 = 'a01_81004'
PREFIX2 = '81004'

QUERY_STR_PARAMS2 = {
    'name': 'fileDown',
    'filetype': 'xls',
    'url': 'MKD/13/1302/13020101/mkd13020101', 
    'market_gubun': 'ALL', 
    'sect_tp_cd': 'ALL',
    'schdate': '',
    'pagePath': '/contents/MKD/13/1302/13020101/MKD13020101.jsp',
}

a01_30017 = Download_per_date(DB, INIT, TB1, PREFIX1, QUERY_STR_PARAMS1)
a01_81004 = Download_per_date(DB, INIT, TB2, PREFIX2, QUERY_STR_PARAMS2)

a01_30017.download(START_DATE, END_DATE)
a01_81004.download(START_DATE, END_DATE)

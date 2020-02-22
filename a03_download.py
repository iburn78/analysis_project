#%%
from ap.classes import Download_per_date
from ap.tools import *
import datetime

DB = 'Analysis'
INIT = 'init'  

START_DATE = '20180101'
END_DATE = '20200221'

TB1 = 'a03_30017'
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

TB2 = 'a03_81004'
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


a30017 = Download_per_date(DB, INIT, TB1, PREFIX1, QUERY_STR_PARAMS1)
a81004 = Download_per_date(DB, INIT, TB2, PREFIX2, QUERY_STR_PARAMS2)

a = datetime.datetime.now()
print("A03_30017 (20180101 to 20200221) Downloading starts at ", a)

a30017.download(START_DATE, END_DATE)

b = datetime.datetime.now()
print("A03_81004 (20180101 to 20200221) Downloading starts at ", b)

a81004.download(START_DATE, END_DATE)

c = datetime.datetime.now()
print("A03 (20180101 to 20200221) Downloading completed at ", c)
print("A03_30017 took ", b-a)
print("A03_81004 took ", c-b)
print("A03 total took ", c-a)


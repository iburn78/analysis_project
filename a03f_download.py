#%%
from ap.classes import Download_per_date
from ap.tools import *
import datetime

DB = 'Analysis'
INIT = 'init'  

START_DATE = '20180101'
END_DATE = '20200221'

TB1 = 'a03f_30017'
PREFIX1 = '30017'
QUERY_STR_PARAMS1 = {
    'name': 'fileDown',
    'filetype': 'xls',
    'url': 'MKD/04/0404/04040400/mkd04040400',
    'stctype': 'ALL',
    'var_invr_cd': '9000',
    'schdate': '',
    'etctype': 'ST',
    'pagePath': '/contents/MKD/04/0404/04040400/MKD04040400.jsp'
}

a30017 = Download_per_date(DB, INIT, TB1, PREFIX1, QUERY_STR_PARAMS1)

a = datetime.datetime.now()
print("A03f_30017 (20180101 to 20200221) Downloading starts at ", a)

a30017.download(START_DATE, END_DATE)

b = datetime.datetime.now()
print("A03f_30017 (20180101 to 20200221) Downloading completed at ", b)
print("A03f_30017 took ", b-a)


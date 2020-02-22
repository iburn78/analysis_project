#%%
from ap.tools import *

DB = 'Analysis'
if 'a3' not in dir():
    [a3, a8] = read_DB_A01(DB)

sdate = '20200102'
edate = '20200110'
target_sdate = '20200113'
target_edate = '20200117'
MKTCAP_COUNT_LIMIT = 1000 # Total number of listed in KOSPI ~700, Kosdaq ~1500
COMPANY_COUNT_LIMIT = 200

dates = [sdate, edate, target_sdate, target_edate]
[am, results, lin_str, weighted_str] = analysis_A01(a3, a8, dates, MKTCAP_COUNT_LIMIT, COMPANY_COUNT_LIMIT)

SAVE_TO_FILE = 'fig.png'

plot_A01(am, results, dates, SAVE_TO_FILE)


#%%
from ap.tools import *
import pandas as pd
import numpy as np
import datetime 
import matplotlib.pyplot as plt

DB = 'Analysis'
if 'a3' not in dir():
    [a3, a8] = read_DB_A02(DB)

MKTCAP_COUNT_LIMIT = 1000 
COMPANY_COUNT_LIMIT = 50

sdate = pd.date_range('20190101', '20191231', freq = 'W-Mon')
edate = sdate + 4*pd.offsets.BDay()

sdate = holiday_skipper(sdate)
edate = holiday_skipper(edate)

dates = pd.DataFrame(columns = ['sdate', 'edate', 'target_sdate', 'target_edate'])
dates.sdate = sdate[:-2]
dates.edate = edate[:-2]
dates.target_sdate = sdate[1:-1]
dates.target_edate = edate[1:-1]

dates2H = dates[dates.sdate >= '20190701']

res2H = []
for i in range(len(dates2H)):
    [am, results, lin_str, weighted_str] = analysis_A01(a3, a8, list(dates2H.iloc[i]), MKTCAP_COUNT_LIMIT, COMPANY_COUNT_LIMIT)
    res2H.append(results)
    SAVE_TO_FILE = f'figs/fig{dates2H.iloc[i][0]}.png'
    plot_A01(am, results, list(dates2H.iloc[i]), SAVE_TO_FILE)


#%% 
lin_str = []
weighted_str = []
for i in range(len(res2H)):
    lin_str.append(res2H[i].Q1[4])
    weighted_str.append(res2H[i].Q1[5])

fig = plt.figure(figsize = (9,6), dpi = 120)
lin_str = np.asarray(lin_str)
weighted_str = np.asarray(weighted_str)
x = list(dates2H.sdate)
for i in range(len(x)):
    x[i] = x[i][4:]
plt.plot(x, lin_str, 'og-', label = "linear strategy return")
plt.plot(weighted_str, '^b-', label = "weighted strategy return")
plt.axhline(0, ls='-', color='grey')
plt.axhline(lin_str.mean(), ls='--', color='g', label = f"lin-average: {lin_str.mean():.2f}%")
plt.axhline(weighted_str.mean(), ls='--', color='b', label = f"weighted-average {weighted_str.mean():.2f}%")
plt.legend(scatterpoints=1, frameon=False, labelspacing=1, loc='upper left')
plt.xlabel("measure period start day")
plt.ylabel("return %")
plt.title(f"2019 2H institutions' weekly {COMPANY_COUNT_LIMIT} top pick - subsequent week performance")
xticks = plt.xticks(rotation=45)

plt.savefig('figs/20192H_summary.png')

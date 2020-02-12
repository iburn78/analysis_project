#%%
from ap.classes import Download_per_date
from ap.tools import *
from matplotlib import cm # colormap
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

DB = 'Analysis'

conn = get_connection(DB)
QUERY = '''
    CREATE VIEW a01_master 
    AS SELECT * FROM a01_30017 A 
    LEFT JOIN a01_81004 B ON 
    A.30017_stockcode = B.81004_stockcode 
    AND A.30017_date = B.81004_date;
'''
cursor = conn.cursor()
# cursor.execute(QUERY) 
# conn.commit()

a3 = pd.read_sql("select * from a01_30017;", conn)
a8 = pd.read_sql("select * from a01_81004;", conn)

cursor.close()
conn.close()

#%%
sdate = '20200102'
edate = '20200110'
label_sdate = '20200113'
label_edate = '20200117'

#### a3 data preparation ####

a3s = a3.set_index('30017_stockcode')[['30017_stockname', '30017_pbuy_net', '30017_date']]
a3s.index.name = 'code'
a3s.columns = ['name', 'pbn', 'date']

# Select data within sdate and edate inclusive
a3s['date'] = pd.to_datetime(a3s['date'])
mask = (a3s['date'] >= sdate) & (a3s['date'] <= edate)
a3s = a3s[mask]

# Group to get summation of price_buy_net for given duration
a3s = a3s.groupby(['code', 'name']).sum()
a3s.reset_index(inplace=True)


#### a8 data preparation ####

a8.rename(columns={"81004_stockcode": "code", "81004_date": "date"}, inplace=True)

a8s = a8[a8['date']==pd.Timestamp(f'{sdate}').date()][['code', '81004_mktcap', '81004_startprice']]
a8s.rename(columns={"81004_mktcap": "mktcap_sdate", "81004_startprice": "startprice_sdate"}, inplace = True)

a8_temp = a8[a8['date']==pd.Timestamp(f'{edate}').date()][['code', '81004_currentprice']]
a8_temp.rename(columns={"81004_currentprice": "price_edate"}, inplace = True)
a8s = pd.merge(a8s, a8_temp, how='left', on=['code'] )

a8_temp = a8[a8['date']==pd.Timestamp(f'{label_sdate}').date()][['code', '81004_startprice']]
a8_temp.rename(columns={"81004_startprice": "startprice_lsdate"}, inplace = True)
a8s = pd.merge(a8s, a8_temp, how='left', on=['code'] )

a8_temp = a8[a8['date']==pd.Timestamp(f'{label_edate}').date()][['code', '81004_currentprice']]
a8_temp.rename(columns={"81004_currentprice": "price_ledate"}, inplace = True)
a8s = pd.merge(a8s, a8_temp, how='left', on=['code'] )


#### a3 and a8 merge ####
# 1. cut 1000 by market size top 
a8s = a8s.sort_values(by=['mktcap_sdate'], ascending=False)[:1000]
# 2. merge and cut 200 by pbn/mktcap
am = pd.merge(a8s, a3s, how='right', on=['code'])
am['pbn_mc'] = am['pbn'] / am['mktcap_sdate']
am = am.sort_values(by=['pbn_mc'], ascending=False)[:200]
# 3. calculate results for the current period and label period
am['cur_res'] = (am['price_edate'] - am['startprice_sdate'])/am['startprice_sdate']
am['label_res'] = (am['price_ledate'] - am['startprice_lsdate'])/am['startprice_lsdate']
am.reset_index(inplace=True)

#%%
# print(plt.style.available) - Lookup for matplotlib styles for more options
plt.style.use('seaborn')
fig, ax = plt.subplots()
ax.grid(True)

ax.axvline(0, ls='--', color='r')
ax.axhline(0, ls='--', color='r')

size = am['mktcap_sdate']/am['mktcap_sdate'].mean()*25

colormap_engine = cm.get_cmap('hot', len(am['pbn_mc'])) # Lookup for matplotlib colormaps for more options
clr = colormap_engine((am['pbn_mc']-am['pbn_mc'].min())/(am['pbn_mc'].max()-am['pbn_mc'].min()))

ax.scatter(am['cur_res'], am['label_res'], c=clr, s=size) 

plt.xlabel(f"current period performance (\'{sdate[2:]}-\'{edate[2:]})")
plt.ylabel(f"target priod performance (\'{label_sdate[2:]}-\'{label_edate[2:]})")
plt.savefig('fig.png')

#%%


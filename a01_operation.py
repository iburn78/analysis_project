#%%
from ap.classes import Download_per_date
from ap.tools import *
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

# Select top 100 in size of pbn 
a3s = a3s.sort_values(by=['pbn'], ascending=False)[:100]

a8.rename(columns={"81004_stockcode": "code", "81004_date": "date"}, inplace=True)

#%%
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

am = pd.merge(a3s, a8s, how='left', on=['code'])

am['cur_res'] = (am['price_edate'] - am['startprice_sdate'])/am['startprice_sdate']
am['label_res'] = (am['price_ledate'] - am['startprice_lsdate'])/am['startprice_lsdate']


am.plot(kind='scatter', x = 'cur_res', y='label_res', color='red')
# plt.savefig('fig.png') 
plt.show()


#%%
import matplotlib.pyplot as plt

fig, axes = plt.subplots(nrows = 2, ncols = 1, sharex=True)
plt.subplot(0,0)
am['cur_res'].plot()
plt.subplot(1,0)
am['label_res'].plot()


#%%
import matplotlib.pyplot as plt
# dataframe sample data
df1 = pd.DataFrame(np.random.rand(10,2)*100, columns=['A', 'B'])
df2 = pd.DataFrame(np.random.rand(10,2)*100, columns=['A', 'B'])
df3 = pd.DataFrame(np.random.rand(10,2)*100, columns=['A', 'B'])
df4 = pd.DataFrame(np.random.rand(10,2)*100, columns=['A', 'B'])
df5 = pd.DataFrame(np.random.rand(10,2)*100, columns=['A', 'B'])
df6 = pd.DataFrame(np.random.rand(10,2)*100, columns=['A', 'B'])
#define number of rows and columns for subplots
nrow=3
ncol=2
# make a list of all dataframes 
df_list = [df1 ,df2, df3, df4, df5, df6]
fig, axes = plt.subplots(nrow, ncol)
# plot counter
count=0
for r in range(nrow):
    for c in range(ncol-1):
        df_list[count].plot(ax=axes[r,c])
        count=+1




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
target_sdate = '20200113'
target_edate = '20200117'

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

a8_temp = a8[a8['date']==pd.Timestamp(f'{target_sdate}').date()][['code', '81004_startprice']]
a8_temp.rename(columns={"81004_startprice": "startprice_tsdate"}, inplace = True)
a8s = pd.merge(a8s, a8_temp, how='left', on=['code'] )

a8_temp = a8[a8['date']==pd.Timestamp(f'{target_edate}').date()][['code', '81004_currentprice']]
a8_temp.rename(columns={"81004_currentprice": "price_tedate"}, inplace = True)
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
am['target_res'] = (am['price_tedate'] - am['startprice_tsdate'])/am['startprice_tsdate']
am.reset_index(inplace=True)
am.drop(columns="index", inplace = True)


#%% 
#### Data Extraction 
am_ct = am[(am['cur_res']>=0) & (am['target_res']>=0)] # Q1
am_t = am[(am['cur_res']<0) & (am['target_res']>=0)] # Q2
am_c = am[(am['cur_res']>=0) & (am['target_res']<0)] # Q3
am_none = am[(am['cur_res']<0) & (am['target_res']<0)] # Q4

count = np.array([len(am_ct), len(am_t), len(am_c), len(am_none)])
prob_count = count/len(am)
total_pbn = np.array([sum(am_ct.pbn), sum(am_t.pbn), sum(am_c.pbn), sum(am_none.pbn)])/10**12 # tKRW  
avg_pbn = total_pbn/count
total_mktcap = np.array([sum(am_ct.mktcap_sdate), sum(am_t.mktcap_sdate), sum(am_c.mktcap_sdate), sum(am_none.mktcap_sdate)])/10**12 # tKRW
avg_mktcap = total_mktcap/count
pbn_mc = avg_pbn/avg_mktcap*100 # Percent

def ar(am, cur=True):
    if cur==True: 
        return sum(am.mktcap_sdate*am.cur_res)/10**12
    else: 
        return sum(am.mktcap_sdate*am.target_res)/10**12

abs_cur_res = np.array([ar(am_ct, 1), ar(am_t, 1), ar(am_c, 1), ar(am_none, 1)]) # tKRW 
abs_target_res = np.array([ar(am_ct, 0), ar(am_t, 0), ar(am_c, 0), ar(am_none, 0)]) # tKRW

results = pd.DataFrame([count, prob_count, avg_mktcap, pbn_mc, abs_cur_res, abs_target_res], columns=['Q1', 'Q2', 'Q3', 'Q4'], 
    index = ['Company Count', 'Probability', 'Avg MktCap(tKRW)', 'Net Buy(%)', 'Abs Cur Inc(tKRW)', 'Abs Target Inc(tKRW)'])


#%%
#########
## FIGURE
#########

# print(plt.style.available) - Lookup for matplotlib styles for more options
plt.style.use('seaborn')

# definitions for the axes
rect_up = np.asarray([0, 0.3, 1, 1])*1
rect_down = np.asarray([0, 0, 1, 0.2])*1

fig = plt.figure(figsize = (9,6), dpi =120)  # fig size (width, height) is in inches, default: [6.4, 4.8] / dpi default is 100

ax_up = plt.axes(rect_up)
ax_down = plt.axes(rect_down)

ax_up.grid(True)
ax_up.axvline(0, ls='--', color='r')
ax_up.axhline(0, ls='--', color='r')

size = am['mktcap_sdate']/10**10 # circle size in number of pixcels in 10*10 KRW
clr = am['pbn_mc']*100 # colormap in percentage

im = ax_up.scatter(am['cur_res'], am['target_res'], c=clr, s=size, cmap='coolwarm', linewidth=0, alpha=1) #use coolwarm_r for reverse colormap
fig.colorbar(im, ax=ax_up, label='Net Purchase Percent(%) on MktCap')

ax_up.text(ax_up.get_xlim()[1],ax_up.get_ylim()[1],'Q1',color='grey', size='10')
ax_up.text(ax_up.get_xlim()[0],ax_up.get_ylim()[1],'Q2',color='grey', size='10')
ax_up.text(ax_up.get_xlim()[0],ax_up.get_ylim()[0],'Q3',color='grey', size='10')
ax_up.text(ax_up.get_xlim()[1],ax_up.get_ylim()[0],'Q4',color='grey', size='10')

ax_up.set_title("Institutions' top pick performance analysis")
ax_up.set_xlabel(f"current period performance (\'{sdate[2:]}-\'{edate[2:]})")
ax_up.set_ylabel(f"target priod performance (\'{target_sdate[2:]}-\'{target_edate[2:]})")

legend_size1 = (size.max()-size.min())*.66+size.min()
legend_size2 = (size.max()-size.min())*.33+size.min()

for l in [legend_size1, legend_size2]:
    ax_up.scatter([], [], c='k', alpha=0.3, s=l, label=str(int(l/10)/10) + ' bKRW')
ax_up.legend(scatterpoints=1, frameon=False, labelspacing=1, title='Mkt Cap')

ax_down.grid(False)
ax_down.axis('off')
ax_down.table(cellText=results.to_numpy(), colLabels=results.columns, loc='center')

plt.savefig('fig.png', bbox_inches='tight')




#%% 

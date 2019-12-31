#%%
from ap.classes import Download_per_date
from ap.tools import *
import matplotlib.pyplot as plt

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
am = pd.read_sql("select * from a01_master", conn)

cursor.close()
conn.close()

#%%

tp = am.set_index('30017_stockcode')[['30017_stockname', '30017_pbuy_net', '81004_mktcap', '81004_increase_rate', '30017_date']]
tp.columns = ['name', 'pbn', 'mktcap', 'upr', 'date']
tp.index.name = 'code'
tp['pbn_in_milwon_mktcap'] = tp['pbn']/tp['mktcap']*10**6

tp.plot(kind='scatter', x = 'pbn_in_milwon_mktcap', y='upr', color='red')
plt.show()
# plt.savefig('fig.png')

import datetime
tp_day1 = tp['date'] == datetime.date(2019, 1, 2)
tp1 = tp[tp_day1]
tp1 = tp1.nlargest(200, columns=['mktcap'])

tp1.plot(kind='scatter', x = 'pbn_in_milwon_mktcap', y='upr', color='blue')
plt.show()



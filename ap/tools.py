#%%
import sys#, pathlib
# sys.path.insert(0, str(pathlib.Path(__file__).parent)) # need only when to run this file independently in jupyter-notebook
import mysql.connector
from mysql.connector import Error
from .setup import *   # explicit relative import cannot be used if this file run indepdendently
import pandas as pd
import numpy as np
from matplotlib import cm # colormap
import matplotlib.pyplot as plt

# rename_fileds() renames fileds of df as defined in fields
# rename_fields() returns the same df 
def rename_fields(df, prefix=''):
    fields['renamed_to'] = prefix + '_' + fields['to'] 
    rename_dict = fields.set_index('from')['renamed_to'].to_dict()
    df = df.rename(columns = rename_dict) 
    return df

def get_connection(db):
    return mysql.connector.connect(host=config.get('AWS_RDS_Host'),
                                database=str(db),
                                user=config.get('user'),
                                password=config.get('password'))

# check_table() checks whether the table exists in database and creates a table as specified in df if needed
# check_table() returns a db connection object 
# df: pandas data frame
# db: database name
# tb: table name
# initialize: 'init' to iniitialzie, otherwise not to initialize 
def check_table_and_connect(df, db, tb, initialize = 'no'):
    try:
        connection = get_connection(db)
        if connection.is_connected():
            cursor = connection.cursor()
            cursor.execute(f"USE {db}")
            cursor.execute(f"SHOW TABLES")
            tables = cursor.fetchall()
            if (f'{tb}', ) in tables and initialize == 'init': # if table exists but wants to initialize 
                cursor.execute(f'drop table {tb}')
                
            if (f'{tb}', ) not in tables or initialize == 'init': # if table does not exist, or when to initialize
                a = fields.set_index('renamed_to').lookup(df.columns, ['type']*len(df.columns))
                query_dict = dict(zip(df.columns, a))
                QUERY = f'CREATE TABLE {tb} ('
                for i in df.columns:
                    QUERY += f'{i} {query_dict[i]}, ' # query_dict[i] is SQL data type for {i}
                QUERY = QUERY[:-2] + ')'
                cursor.execute(QUERY)

            connection.commit() # execute commit to share the modifications with other DB users
            cursor.close()
        return connection

    except Error as e:
        print("Error while connecting to MySQL: ", e)
        return None

# save_to_db() saves df to tb in db via connection
# df: pandas data frame
# db: database name
# tb: table name
def save_to_db(connection, df, db, tb):
    if connection.is_connected():
        cursor = connection.cursor()

        # Think about efficiency
        # outside of all loops
        col_text = '('
        for i in range(len(df.columns)):
            col_text += f'{df.columns[i]}, '
        col_text = col_text[:-2] + ')'

        t = fields.set_index('renamed_to')['type']
        # inside of the loop for records
        for i in df.index:
        # insdie of the loop for columns in a record
            val_text = '('
            for j in df.columns:
                if t[j]  == 'varchar(100)' or t[j] == 'date':
                    val_text += f'\'{df.loc[i][j]}\', '
                elif t[j] == 'float':
                    val_text += f'{df.loc[i][j]}, '
                else: 
                    print('type error')
            val_text = val_text[:-2] + ')'
            INSERT_QUERY = f'INSERT INTO {tb} ' + col_text + ' VALUES ' + val_text + ';'
            cursor.execute(INSERT_QUERY)
            
        connection.commit()
        cursor.close()
        # connection.close()
        return
    else: 
        print('Connection is not connected')
        connection.close()
        sys.exit('Connection ERROR - save_to_db')
        return 

# Simple function to read from SQL Table to DataFrame
def sql_to_df(db, tb):
    try:
        connection = get_connection(db)
        if connection.is_connected():
            query = f"select * from {tb};"
            df = pd.read_sql(query, connection)
            return df
        else: 
            print("Connection is not establisehd")
            return None

    except Error as e:
        print("Error while connecting to MySQL: ", e)
        return None


def query_execution(db, query):
    conn = get_connection(db)
    QUERY = query = '''
        CREATE VIEW a01_master 
        AS SELECT * FROM a01_30017 A 
        LEFT JOIN a01_81004 B ON 
        A.30017_stockcode = B.81004_stockcode 
        AND A.30017_date = B.81004_date;
    '''
    cursor = conn.cursor()
    cursor.execute(QUERY) 
    conn.commit()

    cursor.close()
    conn.close()




#########################################
##### ANAYLSIS 01 RELATED FUNCTIONS #####

def read_DB_A01(db):
    conn = get_connection(db)

    a3 = pd.read_sql("select * from a01_30017;", conn)
    a8 = pd.read_sql("select * from a01_81004;", conn)

    conn.close()
    return [a3, a8]

def read_DB_A02(db):
    conn = get_connection(db)

    a3 = pd.read_sql("select * from a02_30017;", conn)
    a8 = pd.read_sql("select * from a02_81004;", conn)

    conn.close()
    return [a3, a8]

def analysis_A01(a3, a8, dates, MKTCAP_COUNT_LIMIT, COMPANY_COUNT_LIMIT):

    [sdate, edate, target_sdate, target_edate] = dates
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
    a8s = a8s.sort_values(by=['mktcap_sdate'], ascending=False)[:MKTCAP_COUNT_LIMIT]
    # 2. merge and cut 200 by pbn/mktcap
    am = pd.merge(a8s, a3s, how='right', on=['code'])
    am['pbn_mc'] = am['pbn'] / am['mktcap_sdate']
    am = am.sort_values(by=['pbn_mc'], ascending=False)[:COMPANY_COUNT_LIMIT]
    # 3. calculate results for the current period and label period
    am['cur_res'] = (am['price_edate'] - am['startprice_sdate'])/am['startprice_sdate']
    am['target_res'] = (am['price_tedate'] - am['startprice_tsdate'])/am['startprice_tsdate']
    am.reset_index(inplace=True)
    am.drop(columns="index", inplace = True)

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
            return sum(am.mktcap_sdate*am.cur_res/10**12)
        else: 
            return sum(am.mktcap_sdate*am.target_res/10**12)

    # abs_cur_res = np.array([ar(am_ct, 1), ar(am_t, 1), ar(am_c, 1), ar(am_none, 1)]) # tKRW 
    # abs_target_res = np.array([ar(am_ct, 0), ar(am_t, 0), ar(am_c, 0), ar(am_none, 0)]) # tKRW
    lin_str = am.target_res.replace([np.inf, -np.inf], np.nan).dropna().mean()
    weighted_str = am.target_res*am.pbn_mc
    weighted_str = weighted_str.replace([np.inf, -np.inf], np.nan).dropna().sum()/am.pbn_mc.sum()

    total_count = sum(count)
    results = pd.DataFrame([count, prob_count, avg_mktcap, pbn_mc, [lin_str*100], [weighted_str*100]], columns=['Q1', 'Q2', 'Q3', 'Q4'], 
        index = [f'Count({total_count})', 'Prob', 'MktCap(tW)', 'NetBuy(%)', 'liner_return(%)', 'weighted_ret(%)'])

    return [am, results, lin_str, weighted_str]



def plot_A01(am, results, dates, SAVE_TO_FILE):

    # print(plt.style.available) - Lookup for matplotlib styles for more options
    plt.style.use('seaborn')
    [sdate, edate, target_sdate, target_edate] = dates
    total_count = results.iloc[0].sum()

    # definitions for the axes
    rect_up = np.asarray([0, 0.35, 1, 1])*1
    rect_mid = np.asarray([0, 0.15, 1, 0.1])*1
    rect_down = np.asarray([0, 0, 1, 0.1])*1

    fig = plt.figure(figsize = (9,7), dpi =120)  # fig size (width, height) is in inches, default: [6.4, 4.8] / dpi default is 100

    ax_up = plt.axes(rect_up)
    ax_mid = plt.axes(rect_mid)
    ax_down = plt.axes(rect_down)

    ax_up.grid(True)
    ax_up.axvline(0, ls='--', color='r')
    ax_up.axhline(0, ls='--', color='r')

    size = am['mktcap_sdate']/10**10 # circle size in number of pixcels in 10*10 KRW
    clr = am['pbn_mc']*100 # colormap in percentage

    im = ax_up.scatter(am['cur_res'], am['target_res'], c=clr, s=size, cmap='coolwarm', linewidth=0, alpha=1, label=None) #use coolwarm_r for reverse colormap
    fig.colorbar(im, ax=ax_up, label='Net Purchase Percent(%) on MktCap')

    ax_up.text(ax_up.get_xlim()[1],ax_up.get_ylim()[1],'Q1',color='grey', size='10')
    ax_up.text(ax_up.get_xlim()[0],ax_up.get_ylim()[1],'Q2',color='grey', size='10')
    ax_up.text(ax_up.get_xlim()[0],ax_up.get_ylim()[0],'Q3',color='grey', size='10')
    ax_up.text(ax_up.get_xlim()[1],ax_up.get_ylim()[0],'Q4',color='grey', size='10')

    ax_up.set_title(f"Institutions' top pick performance analysis for {total_count} companies")
    ax_up.set_xlabel(f"current period performance ({sdate}-{edate})")
    ax_up.set_ylabel(f"target priod performance ({target_sdate}-{target_edate})")

    ax_mid.grid(False)
    # ax_mid.get_yaxis().set_visible(False)
    ax_mid.patch.set_visible(False) 

    rects = ax_mid.bar(results.columns, results.loc['Prob'])
    ax_mid.set_ylabel("Probability")
    for rect in rects:
        height = rect.get_height()
        ax_mid.text(rect.get_x() + rect.get_width()/2., 1.05*height, '{:.2f}'.format(height), ha='center', va='bottom')

    legend_size1 = (size.max()-size.min())*.66+size.min()
    legend_size2 = (size.max()-size.min())*.33+size.min()

    for l in [legend_size1, legend_size2]:
        ax_up.scatter([], [], c='k', alpha=0.3, s=l, label=str(int(l/10)/10) + ' bKRW')
    ax_up.legend(scatterpoints=1, frameon=False, labelspacing=1, title='Mkt Cap')

    ax_down.grid(False)
    ax_down.axis('off')

    res = results.drop("Prob")
    data = np.round(res.values, 2)
    table = ax_down.table(cellText=data, rowLabels=res.index, loc='center')
    table.auto_set_font_size(False)
    table.set_fontsize(10)

    plt.savefig(SAVE_TO_FILE, bbox_inches='tight')


def holiday_skipper(dates, holidays=HOLIDAYS): 
    res = []
    for i, val in enumerate(dates):
        while val in holidays:
            val += pd.offsets.BDay()
        res.append(val.strftime('%Y%m%d'))
    return res
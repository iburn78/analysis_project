#%%
import requests
import pandas as pd
from io import BytesIO
import mysql.connector
import time
from mysql.connector import Error
import os
import json

with open('config.json') as config_file:
    config = json.load(config_file)

######

DB = 'Analysis'
TB_30017 = 'a01_30017'
# TB_81004 = 'a01_81004'

######

def get_down_30017(pdate):
    gen_req_url = 'http://marketdata.krx.co.kr/contents/COM/GenerateOTP.jspx'
    query_str_params = {
        'name': 'fileDown',
        'filetype': 'xls',
        'url': 'MKD/04/0404/04040400/mkd04040400',
        'stctype': 'ALL',
        'var_invr_cd': '1000',
        'schdate': str(pdate),
        'etctype': 'ST',
        'pagePath': '/contents/MKD/04/0404/04040400/MKD04040400.jsp'
    }
    headers = {
        'Accept': '*/*',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'en-US,en;q=0.9,ko;q=0.8',
        'Connection': 'keep-alive',
        'Host': 'marketdata.krx.co.kr', 
        'Referer': 'http://marketdata.krx.co.kr/mdi',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36',
        'X-Requested-With': 'XMLHttpRequest'
    }
    r = requests.get(gen_req_url, query_str_params, headers=headers)
    time.sleep(2.0)

    if len(r.content) > 0: 
        # print('GET successful for date', pdate)
        pass
    else: 
        print('GET failed for date', pdate)
        return None
    
    gen_req_url_post = 'http://file.krx.co.kr/download.jspx' 
    form_data = {
        'code': r.content,
    }
    headers_post = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'en-US,en;q=0.9,ko;q=0.8',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        'Content-Length': '381',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Cookie': '',
        'Host': 'file.krx.co.kr',
        'Origin': 'http://marketdata.krx.co.kr',
        'Referer': 'http://marketdata.krx.co.kr/mdi',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36',
    }
    r_post = requests.post(gen_req_url_post, form_data, headers=headers_post)

    if len(r_post.content) > 0: 
        # print('POST successful for date', pdate)
        df = pd.read_excel(BytesIO(r_post.content), thousands=',')
        df = df.rename(
            {
               '종목코드': 'stockcode',
               '종목명': 'stockname',
               '매수거래량': 'qbuy',
               '매도거래량': 'qsell',
               '순매수거래량': 'qbuy_net',
               '매수거래대금': 'pbuy',
               '매도거래대금': 'psell',
               '순매수거래대금': 'pbuy_net',
               '업종명': 'business_area',
            }, axis='columns'
        )
        df['date'] = str(pdate)
        # print(df)
        return df
    else: 
        print('POST failed for date', pdate)
        return None


def check_table(db, tb):
    try:
        connection = mysql.connector.connect(host=config.get('AWS_RDS_Host'),
                                            database=str(db),
                                            user=config.get('user'),
                                            password=config.get('password'))
        if connection.is_connected():
            cursor = connection.cursor()
            cursor.execute(f"USE {db}")
            cursor.execute(f"SHOW TABLES")
            tables = cursor.fetchall()
            if (f'{tb}', ) in tables: 
                cursor.close()
                return [connection, 1] # return 1 if table exists
            else: 
                cursor.close()
                return [connection, 0]

    except Error as e:
        print("Error while connecting to MySQL: ", e)

# to initialize table, pass 'init' 
def connect_or_create_table_30017(db, tb, initialize='no'): 
    check = check_table(db, tb)
    cursor = check[0].cursor()
    if initialize == 'init':
        cursor.execute(f'drop table {tb}')
        check[0].commit()

    if check[1] == 0 or initialize == 'init': # if table does not exist, or when to initialize... 
        CREATE_QUERY = f'''
            Create table {tb} (
                stockcode varchar(100), 
                stockname varchar(100),
                qbuy float, 
                qsell float, 
                qbuy_net float, 
                pbuy float, 
                psell float, 
                pbuy_net float, 
                date date)
        '''
        cursor.execute(CREATE_QUERY)
        check[0].commit() 

    cursor.close()
    return check[0]

def save_to_db_30017(connection, df, db, tb):
    if connection.is_connected():
        cursor = connection.cursor()
        for i in range(len(df)):
            a1 = str(df['stockcode'][i])
            a2 = str(df['stockname'][i])
            a3 = float(df['qbuy'][i])
            a4 = float(df['qsell'][i])
            a5 = float(df['qbuy_net'][i]) 
            a6 = float(df['pbuy'][i])
            a7 = float(df['psell'][i])
            a8 = float(df['pbuy_net'][i])
            # a9 = str(df['business_area'][i]) # skipping saving a9 intentionally
            a10 = str(df['date'][i])
            insert_query=f'''
            INSERT INTO {tb}
                        (stockcode, stockname, qbuy, qsell, qbuy_net, pbuy, psell, pbuy_net, date)
            VALUES		('{a1}', '{a2}', {a3}, {a4}, {a5}, {a6}, {a7}, {a8}, '{a10}');
            '''
            cursor.execute(insert_query)
            
        connection.commit()
        cursor.close()
        # connection.close()
        return
    else: 
        print('Connection is not connected')
        # connection.close()
        return 

#%%
conn = connect_or_create_table_30017(DB, TB_30017, 'init')

datelist = pd.date_range(start='20191216', end='20191218')
dates = datelist.strftime("%Y%m%d").tolist()

for date in dates: 
    df = get_down_30017(date)
    if not df.empty:
        save_to_db_30017(conn, df, DB, TB_30017)
        print('Done for ', date)
    else: 
        print('No data for ', date)

conn.close()
print('end of execution')


# %%

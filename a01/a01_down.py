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

def create_table_30017(db, tb):
    check = check_table(db, tb)
    if check[1] == 0: 
        pass # CREATE TABLE 
    
    return check[0]

def save_to_db_30017(df, db, tb):
    try:
        connection = mysql.connector.connect(host=config.get('AWS_RDS_Host'),
                                            database=str(db),
                                            user=config.get('user'),
                                            password=config.get('password'))
        if connection.is_connected():
            cursor = connection.cursor()
            for i in range(len(df)):
                a1 = str(df['inv_name'][i])
                a2 = float(df['qbuy'][i])
                a3 = float(df['qbuy_percent'][i])
                a4 = float(df['qsell'][i])
                a5 = float(df['qsell_percent'][i]) 
                a6 = float(df['qnet_buy'][i])
                a7 = float(df['pbuy'][i])
                a8 = float(df['pbuy_percent'][i])
                a9 = float(df['psell'][i])
                a10 = float(df['psell_percent'][i])
                a11 = float(df['pnet_buy'][i])
                a12 = str(df['date'][i])
                insert_query=f'''
                INSERT INTO table1
                            (inv_name, qbuy, qbuy_percent, qsell, qsell_percent, qnet_buy, pbuy, pbuy_percent, psell, psell_percent, pnet_buy, date)
                VALUES		('{a1}', {a2}, {a3}, {a4}, {a5}, {a6}, {a7}, {a8}, {a9}, {a10}, {a11}, {a12});
                '''
                # print(insert_query)
                cursor.execute(insert_query)
                
            # cursor.execute("SELECT * from table1;")
            # rows = cursor.fetchall()
            # for row in rows: 
            #     for col in row:
            #         print(col)
            connection.commit()
            cursor.close()
            connection.close()
            return

    except Error as e:
        print("Error while connecting to MySQL: ", e)


#%%


check_table('Practice', 'table1')



#%%

datelist = pd.date_range(start='20191001', end='20191015')
dates = datelist.strftime("%Y%m%d").tolist()

for date in dates:
    df = get_stock_by_investor_80019(date)
    if not df.empty:
        save_to_db(df, )
        print('Done for ', date)
    else: 
        print('No data for ', date)


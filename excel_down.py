import requests
import pandas as pd
from io import BytesIO
import mysql.connector
import time
from mysql.connector import Error

def get_stock_by_investor_80019(pdate, stock_cdnm ='A005930', stock_code = 'KR7005930003', stock_name = '삼성전자'):
    gen_req_url = 'http://marketdata.krx.co.kr/contents/COM/GenerateOTP.jspx'
    query_str_params = {
        'name': 'fileDown',
        'filetype': 'xls',
        'url': 'MKD/13/1302/13020303/mkd13020303',
        'isu_cdnm': stock_cdnm+'/'+stock_name, 
        'isu_cd': stock_code, 
        'isu_nm': stock_name,
        'isu_srt_cd': stock_cdnm,
        'period_selector': 'day',
        'fromdate': str(pdate),
        'todate': str(pdate),
        'pagePath': '/contents/MKD/13/1302/13020303/MKD13020303.jsp',
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
        'Content-Length': '545',
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
               '투자자명': 'inv_name',
               '거래량_매수': 'qbuy',
               '거래량_매수구성비': 'qbuy_percent',
               '거래량_매도': 'qsell',
               '거래량_매도구성비': 'qsell_percent',
               '거래량_순매수': 'qnet_buy',
               '거래대금_매수': 'pbuy',
               '거래대금_매수구성비': 'pbuy_percent',
               '거래대금_매도': 'psell',
               '거래대금_매도구성비': 'psell_percent',
               '거래대금_순매수': 'pnet_buy',
            }, axis='columns'
        )
        df['date'] = str(pdate)
        # print(df)
        return df
    else: 
        print('POST failed for date', pdate)
        return None
    
def save_to_db(df):
    try:
        connection = mysql.connector.connect(host='issuetrackerdb.cv2cs77f45ul.ap-northeast-2.rds.amazonaws.com',
                                            database='Practice',
                                            user='admin',
                                            password='passadmin')
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


datelist = pd.date_range(start='20191001', end='20191015')
dates = datelist.strftime("%Y%m%d").tolist()

for date in dates:
    df = get_stock_by_investor_80019(date)
    if not df.empty:
        save_to_db(df)
        print('Done for ', date)
    else: 
        print('No data for ', date)

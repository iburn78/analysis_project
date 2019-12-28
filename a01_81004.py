#%%
import requests
import pandas as pd
from io import BytesIO
from ap import tools
import time


DB= 'Analysis'
TB= 'a01_81004'

START_DATE = '20190102'
END_DATE = '20190103'
INIT = 'init'  # 'init' to initiazlie the DB table, otheriwse not

gen_req_url_get= 'http://marketdata.krx.co.kr/contents/COM/GenerateOTP.jspx'
def query_str_params(pdate):
    return {
    'name': 'fileDown',
    'filetype': 'xls',
    'url': 'MKD/13/1302/13020101/mkd13020101', 
    'market_gubun': 'ALL', 
    'sect_tp_cd': 'ALL',
    'schdate': str(pdate), 
    'pagePath': '/contents/MKD/13/1302/13020101/MKD13020101.jsp',
}

headers_get= {
    'Accept': '*/*',
    'Accept-Encoding': 'gzip, deflate',
    'Accept-Language': 'en-US,en;q=0.9,ko;q=0.8',
    'Connection': 'keep-alive',
    'Cookie': '', 
    'Host': 'marketdata.krx.co.kr', 
    'Referer': 'http://marketdata.krx.co.kr/mdi',
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36',
    'X-Requested-With': 'XMLHttpRequest'
}
   
gen_req_url_post= 'http://file.krx.co.kr/download.jspx'
headers_post= {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate',
    'Accept-Language': 'en-US,en;q=0.9,ko;q=0.8',
    'Cache-Control': 'max-age=0',
    'Connection': 'keep-alive',
    'Content-Length': '500', # may leave this larger than a few cases
    'Content-Type': 'application/x-www-form-urlencoded',
    'Cookie': '',
    'Host': 'file.krx.co.kr',
    'Origin': 'http://marketdata.krx.co.kr',
    'Referer': 'http://marketdata.krx.co.kr/mdi',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36',
}

def get_excel_down(pdate):
    r = requests.get(gen_req_url_get, query_str_params(pdate), headers=headers_get)
    time.sleep(2.0)

    if len(r.content) == 0: 
        print('GET failed for date', pdate)
        return None
    form_data = {'code': r.content,}
    r_post = requests.post(gen_req_url_post, form_data, headers=headers_post)

    if len(r_post.content) > 0: 
        df = pd.read_excel(BytesIO(r_post.content), thousands=',')
        df['date'] = str(pdate)
        df = tools.rename_fields(df)
        return df
    else: 
        print('POST failed for date', pdate)
        return None


start_date = START_DATE
end_date = END_DATE

print('Downloading '+ TB)

datelist = pd.date_range(start=start_date, end=end_date)
dates = datelist.strftime("%Y%m%d").tolist()
df = get_excel_down(start_date)
conn = tools.check_table_and_connect(df, DB, TB, INIT)
if not df.empty:
    tools.save_to_db(conn, df, DB, TB) 
    print('Done for ', start_date)
else: 
    print('No data for ', start_date)

for date in dates[1:]: 
    df = get_excel_down(date)  
    if not df.empty:
        tools.save_to_db(conn, df, DB, TB)
        print('Done for ', date)
    else: 
        print('No data for ', date)

conn.close()
print('end of execution')


#%%
# import sys, pathlib
# sys.path.insert(0, str(pathlib.Path(__file__).parent)) # need only when to run this file independently in jupyter-notebook
import requests 
import pandas as pd
from io import BytesIO
from . import tools
import time

class Download_per_date:
    
    DB= 'Analysis'
    TB= 'table_name'
    PREFIX = 'prefix' # prefix string for each column name of the table
    INIT = 'init'  # 'init' to initiazlie the DB table, otheriwse not
    QUERY_STR_PARAMS = {
        'name': 'fileDown',
        'filetype': 'xls',
    }
    def __init__(self, db, init, tb, prefix, query_str_params):
        self.DB = db
        self.TB = tb
        self.PREFIX = prefix
        self.INIT = init
        self.QUERY_STR_PARAMS = query_str_params

    gen_req_url_get= 'http://marketdata.krx.co.kr/contents/COM/GenerateOTP.jspx'
    def query_str_params(self, pdate):
        self.QUERY_STR_PARAMS['schdate'] = str(pdate)
        return self.QUERY_STR_PARAMS

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

    def get_excel_down(self, pdate):
        r = requests.get(self.gen_req_url_get, self.query_str_params(pdate), headers=self.headers_get)
        time.sleep(2.0)

        if len(r.content) == 0: 
            print('GET failed for date', pdate)
            return None

        r_post = requests.post(self.gen_req_url_post, {'code': r.content,}, headers=self.headers_post)

        if len(r_post.content) > 0: 
            df = pd.read_excel(BytesIO(r_post.content), thousands=',')
            df['date'] = str(pdate)
            df = tools.rename_fields(df, self.PREFIX)
            return df
        else: 
            print('POST failed for date', pdate)
            return None


    def download(self, start_date, end_date): 
        print('Downloading '+ self.TB)

        datelist = pd.date_range(start=start_date, end=end_date, freq='B') # Busienss days
        dates = datelist.strftime("%Y%m%d").tolist()
        df = self.get_excel_down(start_date)
        conn = tools.check_table_and_connect(df, self.DB, self.TB, self.INIT)
        if not df.empty:
            tools.save_to_db(conn, df, self.DB, self.TB) 
            print('Done for ', start_date)
        else: 
            print('No data for ', start_date)

        for date in dates[1:]: 
            df = self.get_excel_down(date)  
            if not df.empty:
                tools.save_to_db(conn, df, self.DB, self.TB)
                print('Done for ', date)
            else: 
                print('No data for ', date)

        conn.close()
        print('end of execution')


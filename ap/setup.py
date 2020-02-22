import pandas as pd

config = {
    "AWS_RDS_Host":"issuetrackerdb.cv2cs77f45ul.ap-northeast-2.rds.amazonaws.com",
    "user":"admin",
    "password":"passadmin"
}

fields = pd.DataFrame(
    [
        ['종목코드', 'stockcode', 'varchar(100)'],
        ['종목명', 'stockname', 'varchar(100)'],
        ['현재가', 'currentprice', 'float'],
        ['대비', 'relative', 'float'],
        ['등락률', 'increase_rate', 'float'],
        ['시가', 'startprice', 'float'],
        ['고가', 'highprice', 'float'],
        ['저가', 'lowprice', 'float'],
        ['거래량', 'qvolume', 'float'],
        ['거래대금', 'pvolume', 'float'],
        ['시가총액', 'mktcap', 'float'],
        ['시가총액비중(%)', 'mktcap_ratio', 'float'],
        ['상장주식수', 'numshare', 'float'],
        ['매수거래량', 'qbuy', 'float'],
        ['매도거래량', 'qsell', 'float'],
        ['순매수거래량', 'qbuy_net', 'float'],
        ['매수거래대금', 'pbuy', 'float'],
        ['매도거래대금', 'psell', 'float'],
        ['순매수거래대금', 'pbuy_net', 'float'],
        ['업종명', 'business_area', 'varchar(100)'],
        ['date', 'date', 'date']
    ],
    columns = ['from', 'to', 'type'])


HOLIDAYS = pd.to_datetime([
    '20190101', '20190204', '20190205', '20190206',
    '20190301', '20190505', '20190512', '20190606',
    '20190815', '20190912', '20190913', '20190914',
    '20191003', '20191009', '20191225' ])
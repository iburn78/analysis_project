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

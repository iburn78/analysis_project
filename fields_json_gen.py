#%%
import json
import pandas as pd

# READ FROM JSON 
# WRITE TO PANDAS AND SAVE IT TO JSON (I may need Json editor for this purpose)
# JSON FILE NAME: fields.json

df = pd.DataFrame(
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
df.to_json('fields.json', orient='split')

  
#%%
# use cases for reference
# print(fields.loc[fields['from'] == '종목명'].to.values)

fields = pd.read_json('fields.json', orient='split')
rename_dict = fields.set_index('from')['to'].to_dict()

#%%

db = pd.read_excel('data.xls', thousands=',')
db = db.rename(columns = rename_dict)

res = fields.set_index('to').lookup(db.columns, ['type']*len(db.columns))
query_dict = dict(zip(db.columns, res))
tb = 'table1'
QUERY = f'CREATE TABLE {tb} ('
for i in db.columns:
    QUERY += f'{i} {query_dict[i]}, '
QUERY = QUERY[:-2] + ')'


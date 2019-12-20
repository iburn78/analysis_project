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
    ], 
    columns = ['from', 'to', 'type'])
df.to_json('fields.json', orient='split')

# %%

fields = pd.read_json('fields.json', orient='split')
print(fields)

test_db = pd.read_excel('data.xls', thousands=',')
# print(test_db)

#%%
for i in test_db.columns:
    
    print(fields)
    print(i)

fields_a.loc[fields['from'] == '종목명'].to

# %%

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
    print(i)

fields.loc[fields['from'] == '종목명'].to.values

# %%
df = test_db 
df = df.rename(columns = fields.set_index('from')['to'].to_dict())

col_text = '('
for i in range(len(df.columns)):
    col_text += f'{df.columns[i]}, '
col_text = col_text[:-2] + ')'

val_text = '('
for i in range(len(df.columns)):
    if fields.set_index('to')[df.columns[i]]
    val_text += 


# for i in range(len(df)):
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

# %%

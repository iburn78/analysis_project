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




'''

#%%
# use cases for reference
# print(fields.loc[fields['from'] == '종목명'].to.values)

fields = pd.read_json('fields.json', orient='split')
rename_dict = fields.set_index('from')['to'].to_dict()

#%%

df = pd.read_excel('data.xls', thousands=',')
df = df.rename(columns = rename_dict)

res = fields.set_index('to').lookup(df.columns, ['type']*len(df.columns))
query_dict = dict(zip(df.columns, res))
tb = 'table1'
QUERY = f'CREATE TABLE {tb} ('
for i in df.columns:
    QUERY += f'{i} {query_dict[i]}, '
QUERY = QUERY[:-2] + ')'

print(QUERY)

# %%
# Think about efficiency

# outside of all loops
col_text = '('
for i in range(len(df.columns)):
    col_text += f'{df.columns[i]}, '
col_text = col_text[:-2] + ')'

t = fields.set_index('to')['type']
c = df.columns
l = len(c)

# inside of the loop for records

r = df.loc[10]
tb = 'TABLE1'

# insdie of the loop for columns in a record
val_text = '('
for i in range(l):
    if t[c[i]]  == 'varchar(100)' or t[c[i]] == 'date':
        val_text += f'\'{r[i]}\', '
    elif t[c[i]] == 'float':
        val_text += f'{r[i]}, '
    else: 
        print('type error')
val_text = val_text[:-2] + ')'
QUERY = f'INSERT INTO {tb} ' + col_text + ' VALUES ' + val_text + ';'

print(QUERY)


'''

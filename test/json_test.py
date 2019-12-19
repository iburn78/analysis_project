#%%
import json
import pandas as pd

# READ FROM JSON 
# WRITE TO PANDAS AND SAVE IT TO JSON (I may need Json editor for this purpose)
# JSON FILE NAME: fields.json

df = pd.DataFrame([['a','b'], ['c','d']], 
index = ['row 1', 'row 2'], columns = ['col 1', 'col 2'])

# %%

df.to_json(orient='split')
pd.read_json(_, orient='split')
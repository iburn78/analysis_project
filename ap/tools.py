#%%
import sys, pathlib
# sys.path.insert(0, str(pathlib.Path(__file__).parent)) # need only when to run this file independently in jupyter-notebook
import mysql.connector
from mysql.connector import Error
import json
from .setup import *   # explicit relative import cannot be used if this file run indepdendently


# rename_fileds() renames fileds of df as defined in fields
# rename_fields() returns the same df 
def rename_fields(df):
    rename_dict = fields.set_index('from')['to'].to_dict()
    df = df.rename(columns = rename_dict) 
    return df



# check_table() checks whether the table exists in database and creates a table as specified in df if needed
# check_table() returns a db connection object 
# df: pandas data frame
# db: database name
# tb: table name
# initialize: 'init' to iniitialzie, otherwise not to initialize 
def check_table_and_connect(df, db, tb, initialize = 'no'):
    try:
        connection = mysql.connector.connect(host=config.get('AWS_RDS_Host'),
                                            database=str(db),
                                            user=config.get('user'),
                                            password=config.get('password'))
        if connection.is_connected():
            cursor = connection.cursor()
            cursor.execute(f"USE {db}")
            cursor.execute(f"SHOW TABLES")
            tables = cursor.fetchall()
            if (f'{tb}', ) in tables and initialize == 'init': # if table exists but wants to initialize 
                cursor.execute(f'drop table {tb}')
                
            if (f'{tb}', ) not in tables or initialize == 'init': # if table does not exist, or when to initialize
                a = fields.set_index('to').lookup(df.columns, ['type']*len(df.columns))
                query_dict = dict(zip(df.columns, a))
    
                QUERY = f'CREATE TABLE {tb} ('
                for i in df.columns:
                    QUERY += f'{i} {query_dict[i]}, ' # query_dict[i] is SQL data type for {i}
                QUERY = QUERY[:-2] + ')'
                cursor.execute(QUERY)

            connection.commit() # execute commit to share the modifications with other DB users
            cursor.close()
        return connection

    except Error as e:
        print("Error while connecting to MySQL: ", e)



# save_to_db() saves df to tb in db via connection
# df: pandas data frame
# db: database name
# tb: table name
def save_to_db(connection, df, db, tb):
    if connection.is_connected():
        cursor = connection.cursor()

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

        for i in range(len(df)):
            r = df.loc[i]

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
            INSERT_QUERY = f'INSERT INTO {tb} ' + col_text + ' VALUES ' + val_text + ';'
            cursor.execute(INSERT_QUERY)
            
        connection.commit()
        cursor.close()
        # connection.close()
        return
    else: 
        print('Connection is not connected')
        connection.close()
        sys.exit('Connection ERROR - save_to_db')
        return 

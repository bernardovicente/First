# -*- coding: latin-1 -*-
"""
Created on Thu Apr 14 13:29:20 2022

@author: SP0042A5
"""

import os
import pandas as pd
import numpy as np
import glob

# ------------- PUT HERE LOCATION OF THE HANA_connect MODULE ----------
os.chdir("C:\\Users\\AROLMCAC\\Desktop\\Data validation\\Python_script_Antoine\\Python script\\Python script\\HANA")
# ---------------------------------------------------------------------
import HANA_connect as hc



def define_query(erp, system='P11', entity=None):
    """
    Function used in table_shape() to determine the correct query to run
    -- erp should be the erp name in the Hana schema (ex: XECC, DATV, SPB1)
    -- entity is the KAPIS of the entity (4 digits for analytics)
    -- system is the system on which to run the query, i.e K11/I11/C11/P11
    """
    
    schema = 'SLT_' + erp + '_' + "X11" + '_3EFB'
    
    try:
        entity = int(entity)
    except (ValueError, TypeError):
        entity = entity
    
    if entity is None:
        query = """SELECT r.SCHEMA_NAME, r.TABLE_NAME, MAX(r.RECORD_COUNT) AS NB_ROWS,COUNT(c.COLUMN_NAME) AS NB_COLS FROM "SYS"."M_TABLES" as r LEFT JOIN SYS.TABLE_COLUMNS as c ON r.SCHEMA_NAME = c.SCHEMA_NAME AND r.TABLE_NAME = c.TABLE_NAME WHERE r.SCHEMA_NAME = '""" + schema + """' GROUP BY r.SCHEMA_NAME, r.TABLE_NAME ORDER BY r.SCHEMA_NAME, r.TABLE_NAME;"""
        status = 'FULL'
    elif type(entity) == int:
        if 1000 < entity < 9999:
            query = """SELECT r.SCHEMA_NAME, r.TABLE_NAME, COUNT(c.COLUMN_NAME) AS NB_COLS FROM "SYS"."M_TABLES" as r LEFT JOIN SYS.TABLE_COLUMNS as c ON r.SCHEMA_NAME = c.SCHEMA_NAME AND r.TABLE_NAME = c.TABLE_NAME WHERE r.SCHEMA_NAME = '""" + schema + """' GROUP BY r.SCHEMA_NAME, r.TABLE_NAME ORDER BY r.SCHEMA_NAME, r.TABLE_NAME;"""
            status = 'PARTIAL'
        else:
           print('Please enter a valid KAPIS, or enter None')
           query = """SELECT 'Please enter a valid KAPIS, or enter None' AS ERROR FROM "SYS"."M_TABLES" LIMIT 1""" 
           status = 'ERROR'
    else:
        print('\nPlease enter a valid KAPIS, or enter None\n')
        query = """SELECT 'Please enter a valid KAPIS, or enter None' AS ERROR FROM "SYS"."M_TABLES" LIMIT 1"""
        status = 'ERROR'
        
    return query, status
        


def table_shape(erp, entity=None, system='P11', filtered=True):
    """
    Function used to determine the number of rows & columns of all tables related to a given ERP (and entity if entity is not None)
    Same arguments than define_query function, except for 'filtered'
    - filtered: True by default, used to filter out the tables for which the number of rows is 0 (for instance if a table exists in a schema but is unused for a given entity)
    """
    
    query, status = define_query(erp=erp, entity=entity, system=system)
    table_shape = hc.run_query(query, system_to_use=system)

    if status == 'ERROR':
        print('\n==> There seems to be an error in the parameters you chose <==\n')
    elif status == 'PARTIAL':
        table_shape['FROM'] = table_shape.SCHEMA_NAME + '.' + table_shape.TABLE_NAME
        table_shape['QUERY'] = """SELECT '""" + table_shape.TABLE_NAME + """' AS TABLE_NAME, COUNT(*) AS NB_ROWS FROM """ + table_shape.FROM + """ WHERE KAPIS = """ + str(entity)
        query_rows = ' UNION '.join(table_shape.QUERY.tolist())
        table_rows = hc.run_query(query_rows, system_to_use=system)
        table_shape = table_shape.merge(table_rows, on='TABLE_NAME')
        table_shape.drop(columns=['FROM', 'QUERY'], inplace=True)
        if filtered:
            table_shape = table_shape.loc[table_shape.NB_ROWS != 0, :]
        print("\nTable shape for {} schema & for entity {} generated\n".format(erp, entity))
    else:
        print("\nTable shape for {} schema generated\n".format(erp))
    
    return table_shape


def compare_env(erp, entity=None, compare=['C11', 'P11']):
    env1 = compare[0]
    env2 = compare[1]
    shape1 = table_shape(erp=erp, entity=entity, filtered=True, system=env1)[['TABLE_NAME', 'NB_ROWS', 'NB_COLS']]
    shape2 = table_shape(erp=erp, entity=entity, filtered=True, system=env2)[['TABLE_NAME', 'NB_ROWS', 'NB_COLS']]
    
    try:
        comp = (shape1==shape2)
        if ((shape1.shape == (1,1)) | (shape2.shape == (1,1))):
            print('Errors occured in at least one the queries.\nComparison:')
            output = False
        elif comp.all().all():
            print('Everything is equal.\nComparison:')
            output = True
        elif comp.all().any():
            print('Some differences appear.\nComparison:')
            output = shape1[['TABLE_NAME']].rename(columns={'TABLE_NAME':'TABLE'}).join(comp.loc[((comp.TABLE_NAME==False) | (comp.NB_ROWS==False) | (comp.NB_COLS==False)), comp.all().loc[comp.all()==False].index.tolist()], how='inner')
        else:
            output = comp
    except ValueError:
        print('Tables do not exist in one of the systems.\nComparison:')
        output = False
   
    return output
    
    
    
    

# CHANGE root_path TO BE THE FOLDER WHERE THE SOURCE DATA IS
def files_shape(root_path="C:\\Users\\AROLMCAC\\Desktop\\Data validation\\Python_script_Antoine\\Python script\\Python script\\Data quality\\SOURCE_DATA",
                file_type='xlsx'):
    """
    Function used to determine the number of rows and columns of the files provided by an entity
    For this to work as efficiently as possible, the files should have the same name as the related Hana tables
    - root_path is the path to the folder containing the data files to analyze
    - file_type is the type of the files to analyse, i.e xlsx or csv (some variants like xl, excel also work)
        => file_type can also be 'mixed' if we have both excels and csv's for a given entity
    """
    
    if file_type == 'mixed':
        paths_csv = glob.glob(root_path + "/*.csv")
        paths_xlsx = glob.glob(root_path + "/*.xlsx")
        paths = paths_csv + paths_xlsx
    else:
        if file_type.lower() == 'excel' or file_type.lower() == 'xl':
            file_type = 'xlsx'
        extension = '.' + file_type.lower()
        paths = glob.glob(root_path + "/*" + extension)
    
    FILE_NAMES = []
    TABLE_NAMES = []
    NB_COLS = []
    NB_ROWS = []
    
    for path in paths:
        file_name = path[len(root_path)+1:]
        FILE_NAMES.append(file_name)
        
        if file_type == 'mixed':
            if path[-4:] == 'xlsx':
                table_name = 'ZBIS_' + file_name.replace('.xlsx', '').upper()
                TABLE_NAMES.append(table_name)
                temp = pd.read_excel(path).shape
            elif path[-4:] == '.csv':
                table_name = 'ZBIS_' + file_name.replace('.csv', '').upper()
                TABLE_NAMES.append(table_name)
                try:
                    temp = pd.read_csv(path,
                                       sep=',',
                                       encoding='latin-1',
                                       low_memory=False).shape
                except pd.errors.ParserError:
                    print('At least one file is unreadable (ParserError): Are you sure you select the correct delimiter and encoding for your csv file?')
                    temp = (0,0)
            else:
                temp = (0,0)
                print('There was an error with file {}'.format(file_name))
        else:
            table_name = 'ZBIS_' + file_name.replace(extension, '').upper()
            TABLE_NAMES.append(table_name)
            if extension == '.xlsx':
                temp = pd.read_excel(path).shape
            elif extension == '.csv':
                try:
                    temp = pd.read_csv(path,
                                       sep=',',
                                       encoding='latin-1',
                                       low_memory=False).shape
                except pd.errors.ParserError:
                    print('At least one file is unreadable (ParserError): Are you sure you select the correct delimiter and encoding for your csv file?')
                    temp = (0,0)
            else:
                print(r"Please select a correct file type: {'csv', 'excel', 'mixed'}")
                temp = (0,0)
            
        NB_ROWS.append(temp[0])
        NB_COLS.append(temp[1])

    files_shape = pd.DataFrame({'FILE_NAME' : FILE_NAMES,
                                'TABLE_NAME' : TABLE_NAMES,
                                'NB_COLS' : NB_COLS,
                                'NB_ROWS' : NB_ROWS})
    
    print('\nFile shape for the source data generated \n')
    
    return files_shape
    

def data_shape_test(erp, entity=None, system='P11', filtered=True, file_type='mixed'):
    """
    Function that runs table_shape() and files_shape() and provides a quick comparison
    Same arguments than the 2 previous functions
    """
    
    try:
        table_shape_df = table_shape(erp=erp, entity=entity, system=system, filtered=filtered)
    except pd.io.sql.DatabaseError:
        print('\nDatabaseError: Please make sure the schema you asked for exists in the selected system')
        table_shape_df = pd.DataFrame({'ERROR':['DatabaseError']})
        
    if table_shape_df.shape != (1,1):
        file_shape_df = files_shape(file_type=file_type)
        
        if table_shape_df.shape[0] == 0:
            print('\nThe table_shape DataFrame is empty, are you sure you selected the correct system and/or KAPIS?')
        elif table_shape_df['NB_ROWS'].sum() == 0:
            print('\nAll tables have 0 rows, are you sure you selected the correct system and/or KAPIS?')
        elif file_shape_df.shape[0] == 0:
            print('\nThe file_shape DataFrame is empty, are you sure you uploaded your files correctly and selected the appropriate file_type?')
        
        table_shape_df.rename(columns={'NB_ROWS':'NB_ROWS_HANA', 'NB_COLS':'NB_COLS_HANA'}, inplace=True)
        file_shape_df.rename(columns={'NB_ROWS':'NB_ROWS_FILE', 'NB_COLS':'NB_COLS_FILE'}, inplace=True)
        
        comp = table_shape_df.merge(file_shape_df, how='outer', on='TABLE_NAME')
        comp['COLS_CHECK'] = np.where(comp.NB_COLS_HANA == comp.NB_COLS_FILE+2, 'ok', 'TO CHECK')
        comp['ROWS_CHECK'] = np.where(comp.NB_ROWS_HANA == comp.NB_ROWS_FILE, 'ok', 'TO CHECK')
        comp['VALIDATED'] = np.where(((comp.COLS_CHECK == 'ok') & (comp.ROWS_CHECK == 'ok')), 'X', '')
        
        print('Comparison recap generated')

        NotValidated = comp.loc[comp.VALIDATED != 'X', 'VALIDATED'].count()
        if NotValidated > 0:
            print('/!\\ Discrepancies observed for {} table(s) /!\\'.format(NotValidated))
    
    else:
        comp = table_shape_df
    
    return comp


# ++++++++++++++++++++++
# END OF DATA SHAPE PART
# ++++++++++++++++++++++


def data_amount_test(erp, table_name, amt_field, entity=None, system='P11',
                     root_path="C:\\Users\\AROLMCAC\\Desktop\\Data validation\\Python_script_Antoine\\Python script\\Python script\\Data quality\\SOURCE_DATA",
                     file_type = 'xlsx'):
    """
    Function to compare easily the amounts in the accounting detail in Hana and in the source file
    - erp, entity, system, root_path, file_type => See above functions
    - table_name: Name of the table in which the amounts to test are. /!\\ The file should have the same name as the table (except the 'ZBIS_' at the start)
    - amt_field: Field to use to sum the amount. /!\\ The field should have the same name in both Hana and the file, and if possible be in uppercase
    """
    
    
    #HANA PART
    schema = 'SLT_' + erp + '_' + "X11" + '_3EFB'
    if table_name[:5] != 'ZBIS_':
        table_name = 'ZBIS_' + table_name
        
    query = """SELECT SUM(""" + amt_field + """) AS HANA_AMT FROM """ + schema + """.""" + table_name
    if entity != None:
        where = """ WHERE KAPIS = """ + str(entity)
        query += where
    
    try:
        hana_amt = hc.run_query(query, system_to_use=system)
    except pd.io.sql.DatabaseError as dberr:
        print('/!\\ DatabaseError: Please check if the arguments used are correct /!\\ \n')
        print(dberr)
        hana_amt = None
    
    #FILE PART
    file_name = table_name[5:]
    if file_type.lower() == 'excel' or file_type.lower() == 'xl':
        file_type = 'xlsx'
    extension = '.' + file_type.lower()
    
    file_path = root_path + '\\' + file_name + extension
    
    if extension == '.xlsx':
        file_amt = pd.read_excel(file_path)
    elif extension == '.csv':
        try:
            file_amt = pd.read_csv(file_path,
                                   sep=',',
                                   encoding='latin-1',
                                   low_memory=False)
        except pd.errors.ParserError:
            print('The file is unreadable (ParserError): Are you sure you select the correct delimiter and encoding for your csv file?')
            file_amt = None
    else:
        print(r"Please select a correct file type: {'csv', 'xlsx'}")
        file_amt = None
        
    if file_amt is not None:
        try:
            file_amt = file_amt[amt_field].sum()
        except (AttributeError, KeyError):
            try:
                amt_field = amt_field.replace(' ', '_').upper()
                file_amt = file_amt[amt_field].sum()
            except (AttributeError, KeyError):
                file_amt = None
    
    if ((file_amt is not None) & (hana_amt is not None)):
        file_amt = pd.DataFrame({'FILE_AMT':[file_amt]})
        comp = hana_amt.join(file_amt)
        comp.index = ['Amount values']
        comp['DELTA'] = comp.HANA_AMT - comp.FILE_AMT
        check = (abs(comp.HANA_AMT - comp.FILE_AMT) < 1).values[0]
        comp['CHECK'] = np.where(check, "TEST OK", "NOK")
        comp.insert(0, 'FIELD_CHECKED', file_name + '.' + amt_field)
    else:
        comp = None
        print('At least one of the amount could not be computed')
    
    return comp

# +++++++++++++++++++++++
# END OF DATA AMOUNT PART
# +++++++++++++++++++++++


#Generate report

erp = 'SPB1'
entity = 1180
table_name = 'ACC_D' #Name of the table for which we want to check the amounts
amt_field =  'LOCAL_AMOUNT' #Field containing the amounts
system = 'P11'
filtered = True
file_type_shape = 'xlsx'
file_type_amt = 'xlsx'
root_path="C:\\Users\\AROLMCAC\\Desktop\\Data validation\\Python_script_Antoine\\Python script\\Python script\\Data quality\\SOURCE_DATA" #Path of the source data

table_shape_df = data_shape_test(erp=erp, entity=entity, system=system, filtered=filtered, file_type=file_type_shape)
sum_amount_df = data_amount_test(erp=erp, table_name=table_name, amt_field=amt_field, entity=entity, system=system, root_path=root_path, file_type=file_type_amt)

output = root_path + '\\data_quality_test_' + erp
if entity is not None:
    output += '-' + str(entity)
output += '.xlsx'

with pd.ExcelWriter(output) as writer:  
    table_shape_df.to_excel(writer, sheet_name='Exhaustivity', index=False)
    sum_amount_df.to_excel(writer, sheet_name='Debit-Credit')



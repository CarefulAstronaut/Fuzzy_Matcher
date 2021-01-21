# -*- coding: utf-8 -*-
"""
Project: Eloqua IDs to DUNS Matching

Author: Mitchell Rea
Date: 2021-01-13
"""

import numpy as np
import pandas as pd
from pyodbc import connect
import teradatasql 

dsn_name = 'gedwi'
qry = (
    """
    SELECT
    	DUNS.DUNS_NBR
    	,DUNS.DUNS_BUS_NAME
    	,DUNS.DUNS_STREET_ADDR
    	,DUNS.DUNS_CITY_NAME
    	,DUNS.DUNS_STE_PROV_NAME
    	,LEFT(DUNS.DUNS_PSTL_CODE, 5)
    
    	,DUNS.NAICS_CODE
    
    	,tax.LEVEL1
    	,tax.LEVEL2
    	,tax.LEVEL3
    	,tax.VERTICAL
    	
    FROM 
    	DDUNSV01.VDUNS_DNB_FACT_FULL duns
    	LEFT JOIN DBSAAD31.MMM_TAXONOMY_V2 tax on tax.NAICS_CD = duns.NAICS_CODE
    	
    WHERE 
    	duns.DUNS_CTRY_NAME = 'USA' AND
    	LEFT(DUNS.DUNS_PSTL_CODE, 5) = ?
    """
)

def execute_small_fetch_query(qry, pc, dsn_name, column_names):
    """
    Executes database query and tidys results
    into a pandas dataframe
    """
    # Establish connection to the database
    connection = connect('DSN=' + dsn_name)
    # Setting UTF-8 gives quicker read time on linux
    connection.setencoding('utf-8')
    db = connection.cursor()

    # Execute the query, pull all records, reformat using provided names
    db.execute(qry, pc)
    records = db.fetchall()
    records_table = pd.DataFrame.from_records(data = records, columns = column_names)

    # Be a good neighbor and don't leave hanging connections
    db.close()
    connection.close()

    return records_table

def eloqua_subset(pc):
    """
    Brings small subset of Eloqua IDs from master file to be used in fuzzy search loop

    Parameters
    ----------
    pc : STRING
        Postal Code to grab the eloqua ids from .csv file.

    Returns
    -------
    E_Table: Dataframe of Eloqua ID's to be matched against DNB_Table

    """
    E_Table = cl_df.loc[cl_df['postal_match'] == '57401']
    
    return E_Table

def duns_subset(pc):
    """
    Brings subset of DNB dataset from gedwi to be fuzzy matched against.
    
    Parameters
    ----------
    pc : STRING
        Postal Code to grab the DNB data from gedwi

    Returns
    -------
    DNB_Table: Dataframe of DNB data to be scored for fuzzy matches from E_Table

    """
    col_names = ['DUNS_NBR', 'BUS_NAME', 'STREET', 'CITY', 'STATE', 'POSTAL_CODE',
                 'NAICS', 'Lvl1', 'Lvl2', 'Lvl3', 'Segment']
    DNB_Table = execute_small_fetch_query(qry, pc, dsn_name, column_names=col_names)
    
    return DNB_Table

#### Main Code ####

## Import Eloqua csv ##

df = pd.read_csv('Eloqua_IDs.csv')
e_df = df.iloc[6173:88505,:] # Only 5-9 digit postal codes; should do a search for string length
e_df['Company'] = e_df['Company'].fillna('NULL Company')
e_df['Address 1'] = e_df['Address 1'].fillna('NULL Address')
e_df['MATCH'] = e_df['Company'] + ' ' + e_df['Address 1']

##Postal Match

eloqua_test = cl_df.loc[cl_df['postal_match'] == '57401']
e_match = eloqua_test['Company'] + ' ' + eloqua_test['Address 1']
eloqua_test['MATCH'] = eloqua_test['Company'] + ' ' + eloqua_test['Address 1']
e_match = eloqua_test.iloc[0, -1]
dnb_test = duns_subset('57401')
dnb_test['MATCH'] = dnb_test['DUNS_NM'] + ' ' + dnb_test['STREET']
dnb_test = dnb_test.reindex(columns = dnb_test.columns.tolist() + ['Ratio', 'Partial', 'TSort', 'TSet'])

for i in range(dnb_test.shape[0]):
    Ratio = fuzz.ratio(e_match.to_string(), dnb_test.iloc[i, 11])
    Partial_Ratio = fuzz.partial_ratio(e_match.to_string(), dnb_test.iloc[i, 11]) 
    Token_Sort_Ratio = fuzz.token_sort_ratio(e_match.to_string(), dnb_test.iloc[i, 11])
    Token_Set_Ratio = fuzz.token_set_ratio(e_match.to_string(), dnb_test.iloc[i, 11])
    
    dnb_test.iloc[i, -1] = Token_Set_Ratio
    dnb_test.iloc[i, -2] = Token_Sort_Ratio
    dnb_test.iloc[i, -3] = Partial_Ratio
    dnb_test.iloc[i, -4] = Ratio


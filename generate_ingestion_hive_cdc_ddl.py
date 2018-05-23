#!/usr/bin/python
'''
to generate hive specific ddls from splice ddl
usage: 
date:
'''
import os
import sys
import csv
import shutil
import string
import time
import datetime
from datetime import timedelta
import re
import logging
import textwrap

ddl_dir='hive_ddl'
shutil.rmtree(ddl_dir)
## create directories
if not os.path.exists(ddl_dir):
       os.makedirs(ddl_dir)
## External Table generator
def hive_ddl_generator(tbl_type,ddl_tokens):
    ddl_tokens = ddl_tokens[:]
    schema,table=ddl_tokens[3].split('.')
    create_str = ' '.join(ddl_tokens[0:3])
    last_col_str = ddl_tokens[-1]
    opt_hive_properties_str = '''\n)\nROW FORMAT DELIMITED\nFIELDS TERMINATED BY '|'\nSTORED AS PARQUET\nTBLPROPERTIES ("auto.purge"="true");\n\n'''
    hst_stg_hive_properties_str = '''\n)\nROW FORMAT DELIMITED\nFIELDS TERMINATED BY '|'\nSTORED AS PARQUET;\n\n'''    
    if tbl_type == 'OPT':
       opt_ddl_file = 'hive_opt_tables_ddl.hql'
       opt_table = 'OPT_' + table
       opt_create_clause = create_str + ' ' + schema + '.' + opt_table + ddl_tokens[4] + '\n'
       del ddl_tokens[0:5]
       del ddl_tokens[-1]
       with open(os.path.join('/home/splice/cetera/sqoop/BONUS1/hive_ddl',opt_ddl_file),'a') as h:
            h.write(opt_create_clause)
            for i in ddl_tokens:
                h.write(i + ',' + '\n')
            h.write(last_col_str)
            h.write(opt_hive_properties_str)
    elif tbl_type == 'HST':
       hst_ddl_file = 'hive_hst_tables_ddl.hql'
       hst_table = 'HST_' + table
       hst_create_clause = create_str + ' ' + schema + '.' + hst_table + ddl_tokens[4] + '\n'
       del ddl_tokens[0:5]
       del ddl_tokens[-1]
       with open(os.path.join('/home/splice/cetera/sqoop/BONUS1/hive_ddl',hst_ddl_file),'a') as k:
            k.write(hst_create_clause)
            for i in ddl_tokens:
                k.write(i + ',' + '\n')
            k.write(last_col_str)
            k.write(hst_stg_hive_properties_str)
    elif tbl_type == 'STG':
       stg_ddl_file = 'hive_stg_tables_ddl.hql'
       stg_table = 'STG_' + table
       stg_create_clause = create_str + ' ' + schema + '.' + stg_table + ddl_tokens[4] + '\n'
       del ddl_tokens[0:5]
       del ddl_tokens[-1]
       with open(os.path.join('/home/splice/cetera/sqoop/BONUS1/hive_ddl',stg_ddl_file),'a') as l:
            l.write(stg_create_clause)
            for i in ddl_tokens:
                l.write(i + ',' + '\n')
            l.write(last_col_str)
            l.write(hst_stg_hive_properties_str)
    else:
       pass
## Internal Table Generator
##def hive_cdc_query_generator(ddl_tokens):
    ## some code


## Splice-Hive data type mappings
SPLICE_TO_HIVE_DTYPE_MAP = {'DATE':'TIMESTAMP','BLOB':'BINARY','CLOB':'STRING','TEXT':'STRING','NUMERIC':'DECIMAL','INTEGER':'INT'}



if len(sys.argv) != 2:
   print("Please pass right number of arguments- Usage : <script> splice-ddl-file")

## Parse the splice ddl file
ddl_token_lst=[]
with open('/home/splice/cetera/sqoop/BONUS1/bonus_splice_ddl.txt','r') as f:
     for line in f:
      if line.startswith('--') or line in ['\n', '\r\n'] or line.strip().startswith('CONSTRAINT'):
         pass
      else:
         if line.startswith('CREATE TABLE'):
            monospaced_create_lst = re.sub(' +',' ',line)       
            create_clause_tokens = monospaced_create_lst.split()
            create_clause_tokens.insert(2,'IF NOT EXISTS')
            ddl_token_lst.extend(create_clause_tokens)
#            print(ddl_token_lst)
         elif line.startswith(') ;') or line.startswith(');') or line.startswith(';') or line.endswith(');'):
#            hive_ddl_generator('EXT',ddl_token_lst)
            hive_ddl_generator('HST',ddl_token_lst)
            hive_ddl_generator('OPT',ddl_token_lst)
            hive_ddl_generator('STG',ddl_token_lst)
            del ddl_token_lst[:]
#            ext_str=")\nROW FORMAT DELIMITED\nFIELDS TERMINATED BY '|'\nSTORED AS TEXTFILE\nLOCATION '/data/FLZ/import/sqoop/" + SCHEMA + "/" + TABLE + "';\n"
#            print(ext_str)
         else:
            monospaced_col_dtype_lst = re.sub(' +',' ',line)
            monospaced_col_dtype_lst1= monospaced_col_dtype_lst.replace(' (','(')
            monospaced_col_dtype_lst2 = monospaced_col_dtype_lst1.replace(' )',')')
            monospaced_col_dtype_lst3 = monospaced_col_dtype_lst2.replace('),',')')
            col_dtype_tokens = ' '.join(monospaced_col_dtype_lst3.split()[0:2])
            col_dtype_tokens = col_dtype_tokens.rstrip(',')
            col,dtype = col_dtype_tokens.split()
            dtype = dtype.upper().replace('DATE',SPLICE_TO_HIVE_DTYPE_MAP['DATE'])
            dtype = dtype.upper().replace('BLOB',SPLICE_TO_HIVE_DTYPE_MAP['BLOB'])
            dtype = dtype.upper().replace('CLOB',SPLICE_TO_HIVE_DTYPE_MAP['CLOB'])
            dtype = dtype.upper().replace('TEXT',SPLICE_TO_HIVE_DTYPE_MAP['TEXT'])
            dtype = dtype.upper().replace('NUMERIC',SPLICE_TO_HIVE_DTYPE_MAP['NUMERIC'])
            dtype = dtype.upper().replace('INTEGER',SPLICE_TO_HIVE_DTYPE_MAP['INTEGER'])
            col = col.strip('"')
            col_dtype_tokens = col + ' ' + dtype
            ddl_token_lst.append(col_dtype_tokens)

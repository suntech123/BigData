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
## create directories
if not os.path.exists(ddl_dir):
       os.makedirs(ddl_dir)
## External Table generator
def hive_ddl_generator(tbl_type,ddl_tokens):
    if tbl_type == 'OPT':
       schema,table=ddl_tokens[3].split('.')
       ddl_file = table + '.hql'
       opt_table = 'OPT_' + table
       create_str = ' '.join(ddl_tokens[0:2])
       create_clause = create_str + ' ' + schema + '.' + opt_table + ddl_tokens[4] + '\n'
       last_col_str = ddl_tokens[-1]
       hive_properties_str = '''\n)\nROW FORMAT DELIMITED\nFIELDS TERMINATED BY '|'\nSTORED AS PARQUET\nTBLPROPERTIES ("auto.purge"="true");'''
       del ddl_tokens[0:5]
       del ddl_tokens[-1]
       with open(os.path.join('/home/splice/cetera/sqoop/BONUS1/hive_ddl',ddl_file),'w') as h:
            h.write(create_clause)
            for i in ddl_tokens:
                h.write(i + ',' + '\n')
            h.write(last_col_str)
            h.write(hive_properties_str)    

## Internal Table Generator
##def hive_cdc_query_generator(ddl_tokens):
    ## some code


## Splice-Hive data type mappings
SPLICE_TO_HIVE_DTYPE_MAP = {'CHAR':'CHAR','VARCHAR':'VARCHAR','DATE':'TIMESTAMP','TIMESTAMP':'TIMESTAMP','BLOB':'BINARY','CLOB':'STRING','TEXT':'STRING','BIGINT':'BIGINT','DECIMAL':'DECIMAL','DOUBLE':'DOUBLE','FLOAT':'FLOAT','INTEGER':'INTEGER','NUMERIC':'DECIMAL','SMALLINT':'SMALLINT','BOOLEAN':'BOOLEAN'}



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
#            hive_ddl_generator('HST',ddl_token_lst)
            hive_ddl_generator('OPT',ddl_token_lst)
            del ddl_token_lst[:]
#            hive_ddl_generator('STG',ddl_token_lst)
#            ext_str=")\nROW FORMAT DELIMITED\nFIELDS TERMINATED BY '|'\nSTORED AS TEXTFILE\nLOCATION '/data/FLZ/import/sqoop/" + SCHEMA + "/" + TABLE + "';\n"
#            print(ext_str)
         else:
            monospaced_col_dtype_lst = re.sub(' +',' ',line)
            monospaced_col_dtype_lst1= monospaced_col_dtype_lst.replace(' (','(')
            monospaced_col_dtype_lst2 = monospaced_col_dtype_lst1.replace(' )',')')
            monospaced_col_dtype_lst3 = monospaced_col_dtype_lst2.replace('),',')')
            col_dtype_tokens = ' '.join(monospaced_col_dtype_lst3.split()[0:2])
            col_dtype_tokens = col_dtype_tokens.rstrip(',')
            ddl_token_lst.append(col_dtype_tokens)

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
    ext_hive_properties_str = "\n)\nROW FORMAT DELIMITED\nFIELDS TERMINATED BY '|'\nSTORED AS TEXTFILE\nLOCATION '/data/FLZ/import/sqoop/" + schema + "/" + table + "'" + ";" +"\n\n" 
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
            h.write(last_col_str + ',' + '\n')
            h.write('REC_NPK_HASH_VALUE' + ' ' + 'BIGINT')
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
            k.write(last_col_str + ',' + '\n')
            k.write('REC_NPK_HASH_VALUE' + ' ' + 'BIGINT')
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
            l.write(last_col_str + ',' + '\n')
            l.write('REC_NPK_HASH_VALUE' + ' ' + 'BIGINT')
            l.write(hst_stg_hive_properties_str)
    elif tbl_type == 'EXT':
       ext_ddl_file = 'hive_ext_tables_ddl.hql'
       ext_table = 'EXT_' + table
       ext_create_clause = create_str + ' ' + schema + '.' + ext_table + ddl_tokens[4] + '\n'
       del ddl_tokens[0:5]
       del ddl_tokens[-1]
       hdfs_dir_stmt_file = 'hive_ext_table_loc_dir_cmds.txt'
       hdfs_loc_dir_stmt = 'hadoop fs -mkdir -p ' + '/data/FLZ/import/sqoop/' +  schema + '/' + table + ';'
       with open(os.path.join('/home/splice/cetera/sqoop/BONUS1/hive_ddl',hdfs_dir_stmt_file),'a') as q:
            q.write(hdfs_loc_dir_stmt + '\n')
       with open(os.path.join('/home/splice/cetera/sqoop/BONUS1/hive_ddl',ext_ddl_file),'a') as m:
            m.write(ext_create_clause)
            for i in ddl_tokens:
                m.write(i.split()[0] + ' ' + 'STRING' + ',' + '\n')
            m.write(last_col_str.split()[0] + ' ' + 'STRING')
            m.write(ext_hive_properties_str)
    else:
       pass
## Internal Table Generator
def hive_cdc_query_generator(keys,ddl_tokens):
    local_ddl_tokens = ddl_tokens[:]
    local_keys = keys[:]
    hash_lst = [ x for x in local_ddl_tokens if x not in local_keys ]
    hash_col_alias = 'HASH' + '(' + ','.join(hash_lst) + ')' + ' ' + 'AS' + ' ' + 'REC_NPK_HASH_VALUE' 
    print(hash_lst)
    print(hash_col_alias)
    print('\n')
    ## some code


## Splice-Hive data type mappings
SPLICE_TO_HIVE_DTYPE_MAP = {'DATE':'TIMESTAMP','BLOB':'BINARY','CLOB':'STRING','TEXT':'STRING','NUMERIC':'DECIMAL','INTEGER':'INT'}



if len(sys.argv) != 2:
   print("Please pass right number of arguments- Usage : <script> splice-ddl-file")

## Parse the splice ddl file
ddl_token_lst = []
key_cols = []
all_cols = []
with open('/home/splice/cetera/sqoop/BONUS1/bonus_splice_ddl.txt','r') as f:
     for line in f:
      if line.startswith('--') or line in ['\n', '\r\n']:
         pass
      elif line.strip().startswith('CONSTRAINT'):
         key_string = line.strip().rsplit(' ',1)[1]
         key_string = key_string.lstrip('(').rstrip(')').strip()
         key_col_lst = key_string.split(',')
         key_cols.extend(key_col_lst)
      else:
         if line.startswith('CREATE TABLE'):
            monospaced_create_lst = re.sub(' +',' ',line)       
            create_clause_tokens = monospaced_create_lst.split()
            create_clause_tokens.insert(2,'IF NOT EXISTS')
            ddl_token_lst.extend(create_clause_tokens)
#            print(ddl_token_lst)
         elif line.startswith(') ;') or line.startswith(');') or line.startswith(';') or line.endswith(');'):
            hive_ddl_generator('HST',ddl_token_lst)
            hive_ddl_generator('OPT',ddl_token_lst)
            hive_ddl_generator('STG',ddl_token_lst)
            hive_ddl_generator('EXT',ddl_token_lst)
            del ddl_token_lst[:]
            hive_cdc_query_generator(key_cols,all_cols)
            del key_cols[:]
            del all_cols[:]
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
            all_cols.append(col)
            col_dtype_tokens = col + ' ' + dtype
            ddl_token_lst.append(col_dtype_tokens)

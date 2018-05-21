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

## External Table generator
def ext_table_generator(ddl_tokens):
    ## something

## Internal Table Generator
def int_table_generator(ddl_tokens):
    ## some code


## Splice-Hive data type mappings
SPLICE_TO_HIVE_DTYPE_MAP = {'CHAR':'CHAR','VARCHAR':'VARCHAR','DATE':'TIMESTAMP','TIMESTAMP':'TIMESTAMP','BLOB':'BINARY','CLOB':'STRING','TEXT':'STRING','BIGINT':'BIGINT','DECIMAL':'DECIMAL','DOUBLE':'DOUBLE','FLOAT':'FLOAT','INTEGER':'INTEGER','NUMERIC':'DECIMAL','SMALLINT':'SMALLINT','BOOLEAN':'BOOLEAN'}



if len(sys.argv) != 2:
   print("Please pass right number of arguments- Usage : <script> splice-ddl-file")

## Parse the splice ddl file
with open('/home/splice/cetera/sqoop/BONUS1/bonus_splice_ddl.txt','r') as f:
     for line in f:
         if line.startswith('--') or line in ['\n', '\r\n']:
            pass
         else:
            if line.startswith('CREATE TABLE'):
               create_list = line.split()
               SCHEMA,TABLE=create_list[2].split('.')
               create_list.insert(2,'IF NOT EXISTS')
               print(' '.join(create_list))
            elif line.startswith(') ;') or line.startswith(');') or line.startswith(';') or line.endswith(');'):
               ext_str=")\nROW FORMAT DELIMITED\nFIELDS TERMINATED BY '|'\nSTORED AS TEXTFILE\nLOCATION '/data/FLZ/import/sqoop/" + SCHEMA + "/" + TABLE + "';\n"
               print(ext_str)
            else:
               print(line.strip().replace(' NOT NULL',''))

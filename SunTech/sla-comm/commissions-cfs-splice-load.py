#!/usr/bin/python
# -*- coding: utf-8 -*-
##############################################################################################################
#
#
#
#
##############################################################################################################
import os
import sys
import csv
import shutil
import string
import time
import datetime
from datetime import timedelta
import re
import ftplib
import logging

## Start logging the progress
class StreamToLogger(object):
   def __init__(self, logger, log_level=logging.INFO):
      self.logger = logger
      self.log_level = log_level
      self.linebuf = ''

   def write(self, buf):
      for line in buf.rstrip().splitlines():
          self.logger.log(self.log_level, line.rstrip())

   def flush(self):
       pass

ets = time.time()
st = datetime.datetime.fromtimestamp(ets).strftime('%Y-%m-%d_%H_%M_%S')
LOG_FILE='/home/splice/cetera/sla_commissions_cfs/logs/commissions-cfs-splice-load_%s.log'%st

logging.basicConfig(filename=LOG_FILE,
                    filemode="w",
                    level=logging.DEBUG,
                    format='%(asctime)s:%(levelname)s:%(name)s:%(message)s'
                    )


stdout_logger = logging.getLogger('STDOUT')
sl = StreamToLogger(stdout_logger, logging.INFO)
sys.stdout = sl

stderr_logger = logging.getLogger('STDERR')
sl = StreamToLogger(stderr_logger, logging.ERROR)
sys.stderr = sl

## Move Local to HDFS
def CleanHdfs(hdfs_dir):
    res=os.popen('sudo -su hdfs hadoop fs -rm -skipTrash %s'%hdfs_dir).read()
    print(res)

def CheckHdfsDirCount(hdfs_dir):
    res=os.popen('sudo -su hdfs hadoop fs -count %s'%hdfs_dir).read()
    lst=res.split()
    return int(lst[1])

## Report splice error
def ThrowError(err_str):
    regex = re.compile(r'ERROR 42Y55|ERROR XIE0M|ERROR XJ001|ERROR SE|ERROR 08006|ClosedChannelException|ERROR 42Y03|ERROR:|ERROR 42Z23|ERROR 22001', re.UNICODE)
    if regex.search(err_str):
       return 1
    else:
       return 0

## Starting Load 
cmd1="""sqlshell.sh<<EOF
TRUNCATE TABLE EDH.MANUAL;
CALL SYSCS_UTIL.IMPORT_DATA ('EDH','MANUAL',null,'/data/DLZ/SLA/SLA_COMMISSIONS/CFS_COMMISSIONS/MANUAL','|',null,null,'yyyy-MM-dd',null,0,'/bad/cc/aqs',true,null);
EOF"""
res1=os.popen(cmd1).read()
if ThrowError(res1) == 1:
   print('There is some discrepencies in source file - Records could not be loaded. Exiting the process\n\n')
   print(res1)
   sys.exit(1)
else:
   pass
print(res1)

cmd2="""sqlshell.sh<<EOF
TRUNCATE TABLE EDH.IPS;
CALL SYSCS_UTIL.IMPORT_DATA ('EDH','IPS',null,'/data/DLZ/SLA/SLA_COMMISSIONS/CFS_COMMISSIONS/IPS','|',null,null,'yyyy-MM-dd',null,0,'/bad/cc/aqs',true,null);
EOF"""
res2=os.popen(cmd2).read()
if ThrowError(res1) == 1:
   print('There is some discrepencies in source file - Records could not be loaded. Exiting the process\n\n')
   print(res2)
   sys.exit(1)
else:
   pass
print(res2)


cmd3="""sqlshell.sh<<EOF
TRUNCATE TABLE EDH.NSCC;
CALL SYSCS_UTIL.IMPORT_DATA ('EDH','NSCC',null,'/data/DLZ/SLA/SLA_COMMISSIONS/CFS_COMMISSIONS/NSCC','|',null,null,'yyyy-MM-dd',null,0,'/bad/cc/aqs',true,null);
EOF"""
res3=os.popen(cmd3).read()
if ThrowError(res3) == 1:
   print('There is some discrepencies in source file - Records could not be loaded. Exiting the process\n\n')
   print(res3)
   sys.exit(1)
else:
   pass
print(res3)


cmd4="""sqlshell.sh<<EOF
TRUNCATE TABLE EDH.PAS_PREMIER;
CALL SYSCS_UTIL.IMPORT_DATA ('EDH','PAS_PREMIER',null,'/data/DLZ/SLA/SLA_COMMISSIONS/CFS_COMMISSIONS/PAS_PREMIER','|',null,null,'yyyy-MM-dd',null,0,'/bad/cc/aqs',true,null);
EOF"""
res4=os.popen(cmd4).read()
if ThrowError(res4) == 1:
   print('There is some discrepencies in source file - Records could not be loaded. Exiting the process\n\n')
   print(res4)
   sys.exit(1)
else:
   pass
print(res4)


cmd5="""sqlshell.sh<<EOF
TRUNCATE TABLE EDH.PERSHING;
CALL SYSCS_UTIL.IMPORT_DATA ('EDH','PERSHING',null,'/data/DLZ/SLA/SLA_COMMISSIONS/CFS_COMMISSIONS/PERSHING','|',null,null,'yyyy-MM-dd',null,0,'/bad/cc/aqs',true,null);
EOF"""
res5=os.popen(cmd5).read()
if ThrowError(res5) == 1:
   print('There is some discrepencies in source file - Records could not be loaded. Exiting the process\n\n')
   print(res5)
   sys.exit(1)
else:
   pass
print(res5)


cmd6="""sqlshell.sh<<EOF
TRUNCATE TABLE EDH.DEBIT_BALANCE;
CALL SYSCS_UTIL.IMPORT_DATA ('EDH','DEBIT_BALANCE',null,'/data/DLZ/SLA/SLA_COMMISSIONS/CFS_COMMISSIONS/DEBIT_BALANCE','|',null,null,'yyyy-MM-dd',null,0,'/bad/cc/aqs',true,null);
EOF"""
res6=os.popen(cmd6).read()
if ThrowError(res6) == 1:
   print('There is some discrepencies in source file - Records could not be loaded. Exiting the process\n\n')
   print(res6)
   sys.exit(1)
else:
   pass
print(res6)


cmd7="""sqlshell.sh<<EOF
TRUNCATE TABLE EDH.CASE_DETAILS;
CALL SYSCS_UTIL.IMPORT_DATA ('EDH','CASE_DETAILS',null,'/data/DLZ/SLA/SLA_COMMISSIONS/CFS_COMMISSIONS/CASE_DETAILS','^A',null,null,null,null,0,'/bad/cc/aqs',true,null);
EOF"""
res7=os.popen(cmd7).read()
if ThrowError(res7) == 1:
   print('There is some discrepencies in source file - Records could not be loaded. Exiting the process\n\n')
   print(res7)
   sys.exit(1)
else:
   pass
print(res7)


## Move the files from local to HDFS for each file set
print("Cleaning HDFS directories...........................")
if CheckHdfsDirCount('/data/DLZ/SLA/SLA_COMMISSIONS/CFS_COMMISSIONS/MANUAL') != 0:
   CleanHdfs('/data/DLZ/SLA/SLA_COMMISSIONS/CFS_COMMISSIONS/MANUAL/*')
if CheckHdfsDirCount('/data/DLZ/SLA/SLA_COMMISSIONS/CFS_COMMISSIONS/IPS') != 0:
   CleanHdfs('/data/DLZ/SLA/SLA_COMMISSIONS/CFS_COMMISSIONS/IPS/*')
if CheckHdfsDirCount('/data/DLZ/SLA/SLA_COMMISSIONS/CFS_COMMISSIONS/NSCC') != 0:
   CleanHdfs('/data/DLZ/SLA/SLA_COMMISSIONS/CFS_COMMISSIONS/NSCC/*')
if CheckHdfsDirCount('/data/DLZ/SLA/SLA_COMMISSIONS/CFS_COMMISSIONS/PAS_PREMIER') != 0:
   CleanHdfs('/data/DLZ/SLA/SLA_COMMISSIONS/CFS_COMMISSIONS/PAS_PREMIER/*')
if CheckHdfsDirCount('/data/DLZ/SLA/SLA_COMMISSIONS/CFS_COMMISSIONS/PERSHING') != 0:
   CleanHdfs('/data/DLZ/SLA/SLA_COMMISSIONS/CFS_COMMISSIONS/PERSHING/*')
if CheckHdfsDirCount('/data/DLZ/SLA/SLA_COMMISSIONS/CFS_COMMISSIONS/DEBIT_BALANCE') != 0:
   CleanHdfs('/data/DLZ/SLA/SLA_COMMISSIONS/CFS_COMMISSIONS/DEBIT_BALANCE/*')
if CheckHdfsDirCount('/data/DLZ/SLA/SLA_COMMISSIONS/CFS_COMMISSIONS/CASE_DETAILS') != 0:
   CleanHdfs('/data/DLZ/SLA/SLA_COMMISSIONS/CFS_COMMISSIONS/CASE_DETAILS/*')


sys.exit(0)

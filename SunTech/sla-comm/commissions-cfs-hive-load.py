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
LOG_FILE='/home/splice/cetera/sla_commissions_cfs/logs/commissions-cfs-data-load_%s.log'%st

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

## Starting Load 
print('=================== Hive ETL started ===================')
os.system('hive -f /home/splice/cetera/sla_commissions_cfs/hive-query/comm_cfs_hive_etl.hql')
print('=================== Hive ETL completed ===================')
##os.system('hive -f /home/splice/cetera/sla_commissions_cfs/hive-query/comm_cfs_hive_etl_manual.hql')


## Move the files from local to HDFS for each file set
print("Cleaning HDFS directories...........................")
if CheckHdfsDirCount('/data/FLZ/import/SLA/SLA_COMMISSIONS/CFS_COMMISSIONS/MANUAL') != 0:
   CleanHdfs('/data/FLZ/import/SLA/SLA_COMMISSIONS/CFS_COMMISSIONS/MANUAL/*')
if CheckHdfsDirCount('/data/FLZ/import/SLA/SLA_COMMISSIONS/CFS_COMMISSIONS/IPS') != 0:
   CleanHdfs('/data/FLZ/import/SLA/SLA_COMMISSIONS/CFS_COMMISSIONS/IPS/*')
if CheckHdfsDirCount('/data/FLZ/import/SLA/SLA_COMMISSIONS/CFS_COMMISSIONS/NSCC') != 0:
   CleanHdfs('/data/FLZ/import/SLA/SLA_COMMISSIONS/CFS_COMMISSIONS/NSCC/*')
if CheckHdfsDirCount('/data/FLZ/import/SLA/SLA_COMMISSIONS/CFS_COMMISSIONS/PAS_PREMIER') != 0:
   CleanHdfs('/data/FLZ/import/SLA/SLA_COMMISSIONS/CFS_COMMISSIONS/PAS_PREMIER/*')
if CheckHdfsDirCount('/data/FLZ/import/SLA/SLA_COMMISSIONS/CFS_COMMISSIONS/PERSHING') != 0:
   CleanHdfs('/data/FLZ/import/SLA/SLA_COMMISSIONS/CFS_COMMISSIONS/PERSHING/*')
if CheckHdfsDirCount('/data/FLZ/import/SLA/SLA_COMMISSIONS/CFS_COMMISSIONS/DEBIT_BALANCE') != 0:
   CleanHdfs('/data/FLZ/import/SLA/SLA_COMMISSIONS/CFS_COMMISSIONS/DEBIT_BALANCE/*')
if CheckHdfsDirCount('/data/FLZ/import/SLA/SLA_COMMISSIONS/CFS_COMMISSIONS/CASE_DETAILS') != 0:
   CleanHdfs('/data/FLZ/import/SLA/SLA_COMMISSIONS/CFS_COMMISSIONS/CASE_DETAILS/*')

sys.exit(0)

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
LOG_FILE='/home/splice/cetera/sla_commissions_cfs/logs/commissions-cfs-file-landing_%s.log'%st


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
def moveLocalToHdfs(local_dir,hdfs_dir):
    res=os.system('sudo -su hdfs hadoop fs -copyFromLocal %s %s'%(local_dir,hdfs_dir))
    if res != 0:
       print "Error Copying files from local to HDFS.....................exiting"
       sys.exit(1)

## Move Network to Local
def copyNetworkToLocal(net_dir,local_dir):
    clnres=os.system('rm -f %s/*.csv'%local_dir)
    os.system('cp %s/*.csv %s/'%(net_dir,local_dir))

## Copying files from mount to local dir
copyNetworkToLocal(r'/home/splice/loadFile/data/Commissions/GenpactTransactions',r'/home/splice/cetera/sla_commissions_cfs/COMM_CFS_GPCT_TRN_LND')

## Copying Salesforce extract Files.
copyNetworkToLocal(r'/home/splice/loadFile/data/Commissions/Salesforce',r'/home/splice/cetera/sla_commissions_cfs/COMM_CFS_SLF_FL_LND')

## Copying Debit Balaces Files.
copyNetworkToLocal(r'/home/splice/loadFile/data/Commissions/Debit_Balances_CFS',r'/home/splice/cetera/sla_commissions_cfs/COMM_CFS_DBL_FL_LND')


## Removing sapaces from Debit Balance files.
for f in os.listdir('/home/splice/cetera/sla_commissions_cfs/COMM_CFS_DBL_FL_LND'):
    old_filename=os.path.join(r"/home/splice/cetera/sla_commissions_cfs/COMM_CFS_DBL_FL_LND", f)
    new_filename = os.path.join(r"/home/splice/cetera/sla_commissions_cfs/COMM_CFS_DBL_FL_LND", re.sub(' +','_',f))
    os.rename(old_filename,new_filename)

## Removing spaces from filenames
for f in os.listdir('/home/splice/cetera/sla_commissions_cfs/COMM_CFS_GPCT_TRN_LND'):
    old_filename=os.path.join(r"/home/splice/cetera/sla_commissions_cfs/COMM_CFS_GPCT_TRN_LND", f)
    new_filename = os.path.join(r"/home/splice/cetera/sla_commissions_cfs/COMM_CFS_GPCT_TRN_LND", re.sub(' +','_',f))   
    os.rename(old_filename,new_filename)

## Removing spaces from salesforce extract csv files
for f in os.listdir('/home/splice/cetera/sla_commissions_cfs/COMM_CFS_SLF_FL_LND'):
    old_filename=os.path.join(r"/home/splice/cetera/sla_commissions_cfs/COMM_CFS_SLF_FL_LND", f)
    new_filename = os.path.join(r"/home/splice/cetera/sla_commissions_cfs/COMM_CFS_SLF_FL_LND", re.sub(' +','_',f))
    os.rename(old_filename,new_filename)

## Removing headers and blank lines from files.
for filename in os.listdir('/home/splice/cetera/sla_commissions_cfs/COMM_CFS_GPCT_TRN_LND'):
    fln=os.path.join(r"/home/splice/cetera/sla_commissions_cfs/COMM_CFS_GPCT_TRN_LND", filename)
    os.system('sed -i "1d" %s'%fln)
    if filename.startswith('Manual_'):
       os.system('sed -i "/,,,,,,,,,,,,,,,/d" %s'%fln)
    elif filename.startswith('Pershing_'):
       os.system('sed -i "/,,,/d" %s'%fln)
    elif filename.startswith('PAS_Premier_'):
       os.system('sed -i "/,,/d" %s'%fln)
    elif filename.startswith('NSCC_'):
       os.system('sed -i "/,,,,,,,,,,,,,,,,/d" %s'%fln)
    elif filename.startswith('IPS_'):
       os.system('sed -i "/,,,,,,,,,,,,,,,,/d" %s'%fln)
    else:
       pass

stg_dir = r'/home/splice/cetera/sla_commissions_cfs/COMM_CFS_STG'
cln_stg = os.system('rm -f %s/*.csv'%stg_dir)

for filename in os.listdir('/home/splice/cetera/sla_commissions_cfs/COMM_CFS_GPCT_TRN_LND'):
    with open(os.path.join(r"/home/splice/cetera/sla_commissions_cfs/COMM_CFS_GPCT_TRN_LND", filename), 'r') as infile:
         reader = csv.reader(infile, quotechar='"', delimiter=',', skipinitialspace=True)
         with open(os.path.join(r"/home/splice/cetera/sla_commissions_cfs/COMM_CFS_STG", filename),'w') as outfile:
              writer = csv.writer(outfile, delimiter='|')
              for row in reader:
                  writer.writerow(row)

## Processing Salesforce files
for filename in os.listdir('/home/splice/cetera/sla_commissions_cfs/COMM_CFS_SLF_FL_LND'):
    with open(os.path.join(r"/home/splice/cetera/sla_commissions_cfs/COMM_CFS_SLF_FL_LND", filename), 'r') as infile:
         reader = csv.reader(infile, quotechar='"', delimiter=',', skipinitialspace=True)
         with open(os.path.join(r"/home/splice/cetera/sla_commissions_cfs/COMM_CFS_STG", filename),'w') as outfile:
              writer = csv.writer(outfile, delimiter='')
              for row in reader:
                  writer.writerow(tuple(s.replace('\n', ' ').replace('\r', '') for s in row))

## Processing Debit Balances Files.
for filename in os.listdir('/home/splice/cetera/sla_commissions_cfs/COMM_CFS_DBL_FL_LND'):
    with open(os.path.join(r"/home/splice/cetera/sla_commissions_cfs/COMM_CFS_DBL_FL_LND", filename), 'r') as infile:
         reader = csv.reader(infile, quotechar='"', delimiter=',', skipinitialspace=True)
         with open(os.path.join(r"/home/splice/cetera/sla_commissions_cfs/COMM_CFS_STG", filename),'w') as outfile:
              writer = csv.writer(outfile, delimiter='|')
              for row in reader:
                  writer.writerow(tuple(s.replace('\n', ' ').replace('\r', '') for s in row))

## Moving Files from STG dir to HDFS dir
for filename in os.listdir('/home/splice/cetera/sla_commissions_cfs/COMM_CFS_STG'):
    fln=os.path.join(r"/home/splice/cetera/sla_commissions_cfs/COMM_CFS_STG", filename)
    if filename.startswith('Manual_'):
       moveLocalToHdfs(fln,'/data/FLZ/import/SLA/SLA_COMMISSIONS/CFS_COMMISSIONS/MANUAL/')
    elif filename.startswith('Pershing_'):
       moveLocalToHdfs(fln,'/data/FLZ/import/SLA/SLA_COMMISSIONS/CFS_COMMISSIONS/PERSHING/')
    elif filename.startswith('PAS_Premier_'):
       moveLocalToHdfs(fln,'/data/FLZ/import/SLA/SLA_COMMISSIONS/CFS_COMMISSIONS/PAS_PREMIER/')
    elif filename.startswith('NSCC_'):
       moveLocalToHdfs(fln,'/data/FLZ/import/SLA/SLA_COMMISSIONS/CFS_COMMISSIONS/NSCC/')
    elif filename.startswith('IPS_'):
       moveLocalToHdfs(fln,'/data/FLZ/import/SLA/SLA_COMMISSIONS/CFS_COMMISSIONS/IPS')
    elif filename.startswith('Commissions_Case_Details'):
       os.system('sed -i "1d" %s'%fln)
       moveLocalToHdfs(fln,'/data/FLZ/import/SLA/SLA_COMMISSIONS/CFS_COMMISSIONS/CASE_DETAILS')
    elif filename.startswith('Negative_Bal_Detail_'):
       os.system('sed -i "1,4d" %s'%fln)
       os.system("sed -i '$d' %s"%fln)
       os.system("sed -i '$d' %s"%fln)
       moveLocalToHdfs(fln,'/data/FLZ/import/SLA/SLA_COMMISSIONS/CFS_COMMISSIONS/DEBIT_BALANCE')
    else:
       pass
    
sys.exit(os.EX_OK)

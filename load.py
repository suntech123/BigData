#!/usr/bin/python
# -*- coding: utf-8 -*-
###############################################################################################################
#
#
#
#
##############################################################################################################
import os
import sys
import shutil
import string
import time
import datetime
import logging
import subprocess
## Initiate logging
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
LOG_FILE='/home/splice/cetera/call-center-agent-detail/logs/call-center-agent-detail_splice_load%s'%st

logging.basicConfig(filename=LOG_FILE,filemode="w",level=logging.DEBUG,format='%(asctime)s:%(levelname)s:%(name)s:%(message)s')

stdout_logger = logging.getLogger('STDOUT')
sl = StreamToLogger(stdout_logger, logging.INFO)
sys.stdout = sl

stderr_logger = logging.getLogger('STDERR')
sl = StreamToLogger(stderr_logger, logging.ERROR)
sys.stderr = sl

## Start loading to splice
print("Currently loading table EDH.AGENT_PERFORMANCE_RPT ................")
cmd1="""sqlshell.sh<<EOF
CALL SYSCS_UTIL.IMPORT_DATA ('EDH','AGENT_PERFORMANCE_RPT',null,'/data/call-center-agent-detail/CALL_CENTER_AGENT_DTL_STG',',',null,null,null,null,0,'/bad/cc/apr',true,null);
EOF"""
res1=os.popen(cmd1).read()
print(res1)

print("EDH.AGENT_PERFORMANCE_RPT loaded successfully.......")
print("Currently loading table EDH.QUEUE_HISTORY ................")
cmd2="""sqlshell.sh<<EOF
CALL SYSCS_UTIL.IMPORT_DATA ('EDH','QUEUE_HISTORY',null,'/data/call-center-agent-detail/CALL_CENTER_QUEUE_HST_STG',',',null,null,null,null,0,'/bad/cc/qh',true,null);
EOF"""
res2=os.popen(cmd2).read()
print(res2)

print("EDH.QUEUE_HISTORY loaded successfully ................")

print("Currently loading table EDH.ALL_QUEUE_SL ................")
cmd3="""sqlshell.sh<<EOF
CALL SYSCS_UTIL.IMPORT_DATA ('EDH','ALL_QUEUE_SL',null,'/data/call-center-agent-detail/CALL_CENTER_QUEUE_SL_STG',',',null,null,null,null,0,'/bad/cc/aqs',true,null);
EOF"""
res3=os.popen(cmd3).read()
print(res3)
print("EDH.ALL_QUEUE_SL loaded successfully ................")

sys.exit(os.EX_OK)

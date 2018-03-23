#!/usr/bin/python
# -*- coding: utf-8 -*-
#################################################################################################################################
#
#
#
################################################################################################################################
import os
import shutil
import sys
import re
import string
## Parameter Intialization
INPUT_FILE=sys.argv[1]
INPUT_FILE_LIST=INPUT_FILE.split('/')
INPUT_FILE_NAME=INPUT_FILE_LIST[-1]
OUTPUT_DIR='/home/splice/cetera/brokerage-operations-quality-center/BROK_OPS_QC_LND_TEMP/'
OUTPUT_FILE_NAME=OUTPUT_DIR+str(INPUT_FILE_NAME)
FILE_CONTENT=''
with open (INPUT_FILE) as f:
    FILE_CONTENT=f.read()
CLEAN_FILE_CONTENT=''
for c in FILE_CONTENT:
    if (c in string.printable ):
        CLEAN_FILE_CONTENT=CLEAN_FILE_CONTENT+c
    else:
        pass
CLEANED_FILE_CONTENT1=re.sub('[Ã‚Â®]','',CLEAN_FILE_CONTENT)
CLEANED_FILE_CONTENT2=re.sub(u"(\u2018|\u2019)", "'",CLEANED_FILE_CONTENT1)
if os.path.exists(OUTPUT_FILE_NAME):
      os.remove(OUTPUT_FILE_NAME)
else:
  pass
with open(OUTPUT_FILE_NAME,'w') as h:
     h.write(CLEANED_FILE_CONTENT2)

sys.exit(0)

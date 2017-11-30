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
import csv
import shutil
import string
import time
from xml.etree.ElementTree import iterparse
import datetime
#from datetime import timedelta
import re
#import logging
#doc = iterparse('/data6/splice/July_Files/DataWarehouse37/DataWarehouse37_FO_USA_M_201608034.xml', ('start', 'end'))
doc = iterparse('/home/splice/cetera/morning-star-dl-load/sample.xml', ('start', 'end'))
path='CUSIP/CUSIP'
path_parts = path.split('/')
#next(doc)
#tag_stack = []
#elem_stack = []
st=datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d_%H_%M_%S')
print(st)
rec=[]
c_rec=[]
for event, elem in doc:
#     nm=elem.find('PackageName')
#     nm=elem.find('InvestmentVehicle')
#     if nm is not None:
#         print(nm.get('_Id'))
#        print(nm.text)
    if event == 'start':
#        nm=elem.find('InvestmentVehicle')
         if elem.tag == 'InvestmentVehicle':
#            print(elem.attrib['_Id'])
            rec.append(elem.attrib['_Id'])
         if elem.tag == 'InvestmentVehicleName':
            if elem.text is None:
               str_val=''
            else:
               str_val=elem.text
            rec.append(str_val)
         if elem.tag == 'FundId':
            if elem.text is None:
               str_val=''
            else:
               str_val=elem.text
            rec.append(str_val)
         if elem.tag == 'ShareClassId':
            if elem.text is None:
               str_val=''
            else:
               str_val=elem.text
            rec.append(str_val)
         if elem.tag == 'LegalType':
            if elem.text is None:
               str_val=''
            else:
               str_val=elem.text
            rec.append(str_val)
         if elem.tag == 'FundFamilyName':
            if elem.text is None:
               str_val=''
            else:
               str_val=elem.text
            rec.append(str_val)
         if elem.tag == 'ShareClassType':
            if elem.text is None:
               str_val=''
            else:
               str_val=elem.text
            rec.append(str_val)
         if elem.tag == 'GlobalCategoryName':
            if elem.text is None:
               str_val=''
            else:
               str_val=elem.text
            rec.append(str_val)
#         if elem.tag == 'GlobalCategoryId':
#            if elem.text is None:
#               str_val=''
#            else:
#               str_val=elem.text
#            rec.append(str_val)
         if elem.tag == 'HoldingDetailId':
            if elem.text is None:
               str_val=''
            else:
               str_val=elem.text
            c_rec.append(str_val)
    if event == 'end':
         if elem.tag == 'InvestmentVehicle':
            rec_dump='|'.join(rec)
            for i in range(0,len(c_rec)):
                print(rec_dump + '|' + str(c_rec[i]))
            rec=[]
            c_rec=[]            
    elem.clear()
#       tag_stack.append(elem.tag)
#       elem_stack.append(elem)
#    elif event == 'end':
#         if tag_stack == path_parts:
#            print(elem)
#            elem_stack[-2].remove(elem)
#         try:
#            tag_stack.pop()
#            elem_stack.pop()
#         except IndexError:
#            pass
ed=datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d_%H_%M_%S')
print(st)
print(ed)
sys.exit(0)

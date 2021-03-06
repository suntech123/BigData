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
from xml.etree.cElementTree import iterparse
import datetime
#from datetime import timedelta
import re
#import logging
#doc = iterparse('/data6/splice/July_Files/DataWarehouse37/DataWarehouse37_FO_USA_M_201608034.xml', ('start', 'end'))
doc = iterparse('/home/kumarsu/sun-000000/DataWarehouse37_VA_USA_D_20171130.xml', ('start', 'end'))
path='CUSIP/CUSIP'
path_parts = path.split('/')
#next(doc)
#tag_stack = []
#elem_stack = []
st=datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d_%H_%M_%S')
#print(st)
path=[]
rec=[]
c_rec=[]
MonthEndTrailingPerformance_Ordered_key=['PerformanceAsOfDate','PerformanceId','ClosePrice','TrailingReturnYTD','TrailingReturnY1','TrailingReturnY2','TrailingReturnY3','TrailingReturnY4','TrailingReturnY5','TrailingReturnSinceInception']
MonthEndTrailingPerformance={'PerformanceAsOfDate':'','PerformanceId':'','ClosePrice':'','TrailingReturnYTD':'','TrailingReturnY1':'','TrailingReturnY2':'','TrailingReturnY3':'','TrailingReturnY4':'','TrailingReturnY5':'','TrailingReturnSinceInception':''}
for event, elem in doc:
#     nm=elem.find('PackageName')
#     nm=elem.find('InvestmentVehicle')
#     if nm is not None:
#         print(nm.get('_Id'))
#        print(nm.text)
    if event == 'start':
#        nm=elem.find('InvestmentVehicle')
         path.append(elem.tag)
         if elem.tag == 'InvestmentVehicle':
#            print(elem.attrib['_Id'])
            rec.append(elem.attrib['_Id'])
         if elem.tag == 'PerformanceAsOfDate' or elem.tag == 'PerformanceId' or elem.tag == 'ClosePrice' or elem.tag == 'TrailingReturnYTD' or elem.tag == 'TrailingReturnY1' or elem.tag == 'TrailingReturnY2' or elem.tag == 'TrailingReturnY3' or elem.tag == 'TrailingReturnY4' or elem.tag == 'TrailingReturnY5' or elem.tag == 'TrailingReturnSinceInception':
#         or elem.tag == 'MarketValue':
            if 'MonthEndTrailingPerformance' in path:
               if elem.text is None:
                  str_val=''
               else:
                  str_val=elem.text
#            str_val_lst.append(str_val)
               MonthEndTrailingPerformance[elem.tag]=str_val 
#            str_val_rec='|'.join(str_val_lst)
#            c_rec.append(str_val_rec)
    if event == 'end':
         if elem.tag == 'MonthEndTrailingPerformance':
            str_val_rec=u'|'.join(MonthEndTrailingPerformance[x] for x in MonthEndTrailingPerformance_Ordered_key).encode('utf-8').strip()
            c_rec.append(str_val_rec)
            MonthEndTrailingPerformance=dict.fromkeys(MonthEndTrailingPerformance,'')
         if elem.tag == 'InvestmentVehicle':
            rec_dump='|'.join(rec)
            for i in range(0,len(c_rec)):
                print(rec_dump + '|' + str(c_rec[i]))
            del rec[:]
            del c_rec[:]
         path.pop()               
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

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
doc = iterparse('/data6/splice/July_Files/DataWarehouse37/DataWarehouse37_FO_USA_M_201608034.xml', ('start', 'end'))
path='CUSIP/CUSIP'
path_parts = path.split('/')
#next(doc)
#tag_stack = []
#elem_stack = []
st=datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d_%H_%M_%S')
print(st)
for event, elem in doc:
#     nm=elem.find('PackageName')
#     nm=elem.find('InvestmentVehicle')
#     if nm is not None:
#         print(nm.get('_Id'))
#        print(nm.text)
    if event == 'start':
#        nm=elem.find('InvestmentVehicle')
         if elem.tag == 'InvestmentVehicle':
            print(elem.attrib['_Id'])
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
print(ed)
sys.exit(0)

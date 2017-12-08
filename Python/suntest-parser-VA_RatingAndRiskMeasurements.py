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
RatingAndRiskMeasurements_Ordered_key=['RiskRatingOverall' ,'RiskScoreOverall' ,'Volatility' ,'ArithmeticMeanY1' ,'StandardDeviationY1' ,'SkewnessY1' ,'KurtosisY1' ,'SharpeRatioY1' ,'SortinoRatioY1' ,'TrackingErrorY1' ,'InformationRatioY1' ,'CaptureRatioUpY1' ,'CaptureRatioDownY1' ,'BattingAverageY1' ,'ArithmeticMeanY3' ,'StandardDeviationY3' ,'SkewnessY3' ,'KurtosisY3' ,'SharpeRatioY3' ,'SortinoRatioY3' ,'TrackingErrorY3' ,'InformationRatioY3' ,'CaptureRatioUpY3' ,'CaptureRatioDownY3' ,'BattingAverageY3' ,'ArithmeticMeanY5' ,'StandardDeviationY5' ,'SkewnessY5' ,'KurtosisY5' ,'SharpeRatioY5' ,'SortinoRatioY5' ,'TrackingErrorY5' ,'InformationRatioY5' ,'CaptureRatioUpY5' ,'CaptureRatioDownY5' ,'BattingAverageY5']
RatingAndRiskMeasurements={'RiskRatingOverall':'','RiskScoreOverall':'','Volatility':'','ArithmeticMeanY1':'','StandardDeviationY1':'','SkewnessY1':'','KurtosisY1':'','SharpeRatioY1':'','SortinoRatioY1':'','TrackingErrorY1':'','InformationRatioY1':'','CaptureRatioUpY1':'','CaptureRatioDownY1':'','BattingAverageY1':'','ArithmeticMeanY3':'','StandardDeviationY3':'','SkewnessY3':'','KurtosisY3':'','SharpeRatioY3':'','SortinoRatioY3':'','TrackingErrorY3':'','InformationRatioY3':'','CaptureRatioUpY3':'','CaptureRatioDownY3':'','BattingAverageY3':'','ArithmeticMeanY5':'','StandardDeviationY5':'','SkewnessY5':'','KurtosisY5':'','SharpeRatioY5':'','SortinoRatioY5':'','TrackingErrorY5':'','InformationRatioY5':'','CaptureRatioUpY5':'','CaptureRatioDownY5':'','BattingAverageY5':''}
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
         if elem.tag=='RiskRatingOverall'or elem.tag=='RiskScoreOverall'or elem.tag=='Volatility'or elem.tag=='ArithmeticMeanY1'or elem.tag=='StandardDeviationY1'or elem.tag=='SkewnessY1'or elem.tag=='KurtosisY1'or elem.tag=='SharpeRatioY1'or elem.tag=='SortinoRatioY1'or elem.tag=='TrackingErrorY1'or elem.tag=='InformationRatioY1'or elem.tag=='CaptureRatioUpY1'or elem.tag=='CaptureRatioDownY1'or elem.tag=='BattingAverageY1'or elem.tag=='ArithmeticMeanY3'or elem.tag=='StandardDeviationY3'or elem.tag=='SkewnessY3'or elem.tag=='KurtosisY3'or elem.tag=='SharpeRatioY3'or elem.tag=='SortinoRatioY3'or elem.tag=='TrackingErrorY3'or elem.tag=='InformationRatioY3'or elem.tag=='CaptureRatioUpY3'or elem.tag=='CaptureRatioDownY3'or elem.tag=='BattingAverageY3'or elem.tag=='ArithmeticMeanY5'or elem.tag=='StandardDeviationY5'or elem.tag=='SkewnessY5'or elem.tag=='KurtosisY5'or elem.tag=='SharpeRatioY5'or elem.tag=='SortinoRatioY5'or elem.tag=='TrackingErrorY5'or elem.tag=='InformationRatioY5'or elem.tag=='CaptureRatioUpY5'or elem.tag=='CaptureRatioDownY5'or elem.tag=='BattingAverageY5':
#         or elem.tag == 'MarketValue':
            if 'RatingAndRiskMeasurements' in path:
               if elem.text is None:
                  str_val=''
               else:
                  str_val=elem.text
#            str_val_lst.append(str_val)
               RatingAndRiskMeasurements[elem.tag]=str_val 
#            str_val_rec='|'.join(str_val_lst)
#            c_rec.append(str_val_rec)
    if event == 'end':
         if elem.tag == 'RatingAndRiskMeasurements':
            str_val_rec=u'|'.join(RatingAndRiskMeasurements[x] for x in RatingAndRiskMeasurements_Ordered_key).encode('utf-8').strip()
            c_rec.append(str_val_rec)
            RatingAndRiskMeasurements=dict.fromkeys(RatingAndRiskMeasurements,'')
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

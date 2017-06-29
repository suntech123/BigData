#!/usr/bin/python
# -*- coding: utf-8 -*-
#########################################################################################################
#
#
#
#
#
########################################################################################################
import string
import re
import os
import sys
## Create a dictionary object to store formulla keys--Added in new logic
dict_keys = {'A':'% of Principal Value','B':'Surcharge, % of Current Commission','C':'$ Per share or Per Bond or Per Contract','D':'Flat Dollar Amount','E':'$ Per Round Lot','F':'% of Principal After Prior Level Range','G':'$ Per Share After Prior Level Range','H':'$ Per (next) Round Lot After Prior Level','J':'$ Per (current) Round Lot After Prior Level','N':'Non-Overriding Minimum (Apply Min; Check Pre Fig)','O':'Overriding Minimum (Apply Min, Check Pre-fig & Discount)','P':'Absolute Minimum (Apply Min; Stop)','S':'Sum Up to Commissions','T':'Table Values','W':'Absolute Maximum (Apply Max; Stop)','X':'Maximum (Apply Max; Check Pre-Fig & Discount)','Y':'Apply Discount; Check for Max and Pre-fig','Z':'Return to Pre-May Routine'}

## Read the entire file as a single string buffer
with open('/home/kumarsu/dol-mainframe-parsing/IBD-157-Commissions-011514.txt', 'rt') as f:
    data = f.read()
    f.close()

## Replace the header for each record set
rep_pat = re.compile(r'^\-+\s+PROFILE AND COMMISSION SCHEDULE DETAIL REPORT\s+\-+$', re.MULTILINE)
#rep_pat = re.compile(r'^\-+\s+PROFILE AND COMMISSION SCHEDULE DETAIL REPORT\s+\-+$', re.MULTILINE)
data = rep_pat.sub('PROFILE AND COMMISSION SCHEDULE DETAIL REPORT',data)

## Remove all blank lines
data = os.linesep.join([s for s in data.splitlines() if s.strip()])

## Write the file back to the disk as file to process
with open('/home/kumarsu/dol-mainframe-parsing/output/sun4.txt', 'wt') as f:
     f.write(data)
     f.close()

## Reading data from buffer object to feed and serialize bd_detail objects to csv file
## ---Added in v1 for testing
count = 100
tagged_lines = []
with open('/home/kumarsu/dol-mainframe-parsing/output/sun4.txt') as h:
## ----Added in v1 for testing
     for line in h:
         if line.startswith('PROFILE AND COMMISSION SCHEDULE DETAIL REPORT'):
            count += 1
            line = str(count) + line
         tagged_lines.append(line)
lst_line = str(count + 1) + 'PROFILE AND COMMISSION SCHEDULE DETAIL REPORT'
tagged_lines.append(lst_line)
## -----Added in v1 for testing
tagged_str = ''.join(tagged_lines)

with open('/home/kumarsu/dol-mainframe-parsing/output/sun4.txt') as h:
     input_data = h.read()
     h.close()

## Creating data set lst
result = []
for i in range(101,1125):
    strt = str(i) + 'PROFILE AND COMMISSION SCHEDULE DETAIL REPORT'
    end = str(i+1) + 'PROFILE AND COMMISSION SCHEDULE DETAIL REPORT'
#    my_regex = re.escape(strt) + r'(.*?)' + re.escape(end)
    str_extrct = re.search(strt+'(.*?)'+end, tagged_str, re.S)
    result.append(str_extrct.group(1))

#result = re.findall('PROFILE AND COMMISSION SCHEDULE DETAIL REPORT(.*?)PROFILE AND COMMISSION SCHEDULE DETAIL REPORT', input_data, re.S)

#print(tagged_lines)
#for i in tagged_lines:
#    print(i)
print 'BD|BD NAME|PROFILE ID|COMM ID|ACCOUNT|OFFICE|RR THRU|RR|CUSIP|MKT BLOT|BS CODE|ACCT CATG|SOI|TICKET TYPE|PRODUCT|ACCT TYPE|field1|field2|field3|field4|field5|field6|field7|field8|field9|field10|field12|field13|field14|field15|field16|field17|field18|field19|field20|field21|field22|field23|field24|field25|field26|field27|field28|field29|field30|field31|field32|field33|field34|field35'

#max = 0
for str1 in result:
    p1 = re.compile('(BD\s+:\s+)(\w+)')
    p2 = re.compile('(BD NAME\s+:\s+)(\w+\s+\w+\s+\w+\s+\w+)')
    p3 = re.compile('(PROFILE ID\s*:\s+)([a-zA-Z0-9*$./:\-%@#¢]*)')
    p4 = re.compile('(COMM\. ID\s+:\s+)([a-zA-Z0-9*$./:\-%@#¢]*)')
#    p5 = re.compile('(OFFICE\s+:\s+)([a-zA-Z0-9*$./:\-%@#]+[\s])')
    p5 = re.compile('(OFFICE\s+:\s+)(.*?)(CUSIP\s+:)',re.S)
    p6 = re.compile('(CUSIP\s+:)(.*)')
    p7 = re.compile('(B[/]S CODE\s+:\s+)(.*)(ACCT CATG  :)')
    p8 = re.compile('(ACCT CATG\s+:)(.*)')
    p9 = re.compile('(SOI\s+:\s+)(.*)(TICKET TYPE:)')
    p10 = re.compile('(PRODUCT\s+:\s+)(.*)(ACCT TYPE  :)')
    p11 = re.compile('(ACCT TYPE\s+:)(.*)')
    p12 = re.compile('(TICKET TYPE\s*:\s+)([a-zA-Z0-9*$./:\-%@#¢]*)')
    p13 = re.compile('(ACCOUNT\s+:\s+)(.*?)(CUSIP\s+:)',re.S)
    p14 = re.compile('(MKT[/]BLOT\s+:)(.*?)(B[/]S CODE\s+:)',re.S)
    p15 = re.compile('(RR  THRU\s+:)(.*?)(CUSIP\s+:)',re.S)
    p16 = re.compile('(RR\s+:)(.*?)(CUSIP\s+:)',re.S)
    match_bd = re.search(p1,str1)
    match_bd_name = re.search(p2,str1)
    match_profile_id = re.search(p3,str1)
    match_comm_id = re.search(p4,str1)
    if re.search(p16,str1):
       match_rr = re.search(p16,str1)
       match_rr = match_rr.group(2)
       match_rr = match_rr.rstrip('\n').strip()
       match_rr = match_rr.replace('\n', '')
       match_rr = match_rr.replace('              ', ' ')
    else:
       match_rr = ''
    if re.search(p15,str1):
       match_rthru = re.search(p15,str1)
       match_rthru = match_rthru.group(2)
       match_rthru = match_rthru.rstrip('\n').strip()
       match_rthru = match_rthru.replace('\n', '')
       match_rthru = match_rthru.replace('              ', ' ')
    else:
       match_rthru = ''
    if re.search(p14,str1):
       match_mkt = re.search(p14,str1)
       match_mkt = match_mkt.group(2)
       match_mkt = match_mkt.rstrip('\n').strip()
       match_mkt = match_mkt.replace('\n', '')
       match_mkt = match_mkt.replace('              ', ' ')
    else:
       match_mkt = ''
    if re.search(p13,str1):
       match_account = re.search(p13,str1)
       match_account = match_account.group(2)
       match_account = match_account.rstrip('\n').strip()
       match_account = match_account.replace('\n', '')
       match_account = match_account.replace('              ', ' ')
    else:
       match_account = ''
    if re.search(p12,str1):
       match_tkt_type = re.search(p12,str1)
       match_tkt_type = match_tkt_type.group(2)
       match_tkt_type = match_tkt_type.rstrip('\n').strip()
    else:
       match_tkt_type = ''
    if re.search(p5,str1):
       match_office = re.search(p5,str1)
       match_office = match_office.group(2)
       match_office = match_office.rstrip('\n').strip()
       match_office = match_office.replace('\n', '')
       match_office = match_office.replace('              ', ' ')      
    else:
       match_office = ''
    if re.search(p6,str1):
       match_cusip = re.search(p6,str1)
       match_cusip = match_cusip.group(2)
    else:
       match_cusip = ''
    if re.search(p7,str1):
       match_bscd = re.search(p7,str1)
       match_bscd = match_bscd.group(2)
    else:
       match_bscd = ''
    if re.search(p8,str1):
       match_accat = re.search(p8,str1)
       match_accat = match_accat.group(2)
    else:
       match_accat = ''

    if re.search(p9,str1):
       match_soi = re.search(p9,str1)
       match_soi = match_soi.group(2)
    else:
       match_soi = ''

    if re.search(p10,str1):
       match_prd = re.search(p10,str1)
       match_prd = match_prd.group(2)
    else:
       match_prd = ''

    if re.search(p11,str1):
       match_actype = re.search(p11,str1)
       match_actype = match_actype.group(2)
    else:
       match_actype = ''
    ReadMode = False
    for line in str1.splitlines():
        if ReadMode:
           sline = line.lstrip()
           linewd = re.sub(' +','|',sline)
           rw = linewd.split('|')           
           lent = len(rw)
           fmt_str = '|%s' * lent
           str_val = ''
           for i in range(0,lent):
               rw[i] = rw[i].strip()
               str_val += dict_keys.get(rw[i],rw[i]) + '|' 
           print "%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s" % (match_bd.group(2), match_bd_name.group(2),match_profile_id.group(2).rstrip('\n').strip(),match_comm_id.group(2).rstrip('\n').strip(),match_account,match_office.rstrip('\n').strip(),match_rthru,match_rr,match_cusip.rstrip('\n').strip(),match_mkt,match_bscd.rstrip('\n').strip(),match_accat.rstrip('\n').strip(),match_soi.rstrip('\n').strip(),match_tkt_type,match_prd.rstrip('\n').strip(),match_actype.rstrip('\n').strip(),str_val.rstrip('|'))
#           print "%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s" % (match_bd.group(2), match_bd_name.group(2),match_profile_id.group(2).rstrip('\n').strip(),match_comm_id.group(2).rstrip('\n').strip(),match_office.rstrip('\n').strip(),match_cusip.rstrip('\n').strip(),match_bscd.rstrip('\n').strip(),match_accat.rstrip('\n').strip(),match_soi.rstrip('\n').strip(),match_tkt_type,match_prd.rstrip('\n').strip(),match_actype.rstrip('\n').strip(),str_val.rstrip('|'))
        if line.startswith('--------------------'):
           ReadMode = True


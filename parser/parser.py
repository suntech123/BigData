#!/usr/bin/python

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

## Read the entire file as a single string buffer
with open('/home/kumarsu/dol-mainframe-parsing/IBD-3KZ.txt', 'rt') as f:
    data = f.read()

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

with open('/home/kumarsu/dol-mainframe-parsing/output/sun4.txt') as h:
     input_data = h.read()

result = re.findall('PROFILE AND COMMISSION SCHEDULE DETAIL REPORT(.*?)PROFILE AND COMMISSION SCHEDULE DETAIL REPORT', input_data, re.S)

#print(result[100])
print 'BD|BD NAME|PROFILE ID|COMM ID|OFFICE|CUSIP|BS CODE|ACCT CATG|SOI|PRODUCT|ACCT TYPE|field1|field2|field3|field4|field5|field6|field7|field8|field9|field10|field12|field13|field14|field15|field16|field17|field18|field19|field20|field21|field22|field23|field24|field25|field26|field27|field28|field29|field30|field31|field32|field33|field34|field35'
#max = 0
for str in result:
    p1 = re.compile('(BD\s+:\s+)(\w+)')
    p2 = re.compile('(BD NAME\s+:\s+)(\w+\s+\w+\s+\w+)')
    p3 = re.compile('(PROFILE ID\s*:\s+)([a-zA-Z0-9*$./:\-%@#]*)')
    p4 = re.compile('(COMM\. ID\s+:\s+)([a-zA-Z0-9*$./:\-%@#]*)')
#    p5 = re.compile('(OFFICE\s+:\s+)([a-zA-Z0-9*$./:\-%@#]+[\s])')
    p5 = re.compile('(OFFICE\s+:\s+)(.*)')
    p6 = re.compile('(CUSIP\s+:)(.*)')
    p7 = re.compile('(B[/]S CODE\s+:\s+)(.*)(ACCT CATG  :)')
    p8 = re.compile('(ACCT CATG\s+:)(.*)')
    p9 = re.compile('(SOI\s+:\s+)(.*)(TICKET TYPE:)')
    p10 = re.compile('(PRODUCT\s+:\s+)(.*)(ACCT TYPE  :)')
    p11 = re.compile('(ACCT TYPE\s+:)(.*)')
    match_bd = re.search(p1,str)
    match_bd_name = re.search(p2,str)
    match_profile_id = re.search(p3,str)
    match_comm_id = re.search(p4,str)
    if re.search(p5,str):
       match_office = re.search(p5,str)
       match_office = match_office.group(2)
    else:
       match_office = ''
    if re.search(p6,str):
       match_cusip = re.search(p6,str)
       match_cusip = match_cusip.group(2)
    else:
       match_cusip = ''
    if re.search(p7,str):
       match_bscd = re.search(p7,str)
       match_bscd = match_bscd.group(2)
    else:
       match_bscd = ''
    if re.search(p8,str):
       match_accat = re.search(p8,str)
       match_accat = match_accat.group(2)
    else:
       match_accat = ''

    if re.search(p9,str):
       match_soi = re.search(p9,str)
       match_soi = match_soi.group(2)
    else:
       match_soi = ''

    if re.search(p10,str):
       match_prd = re.search(p10,str)
       match_prd = match_prd.group(2)
    else:
       match_prd = ''

    if re.search(p11,str):
       match_actype = re.search(p11,str)
       match_actype = match_actype.group(2)
    else:
       match_actype = ''

#   print "%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s" % (match_bd.group(2), match_bd_name.group(2),match_profile_id.group(2),match_comm_id.group(2),match_office.rstrip('\n'),match_cusip.rstrip('\n'),match_bscd.strip(),match_accat.rstrip('\n'),match_soi.strip(),match_prd.strip(),match_actype.rstrip('\n'))
    ReadMode = False
    for line in str.splitlines():
        if ReadMode:
           linewd = re.sub(' +','|',line)
#          num = len(linewd) + 11
#           if max < num:
#              max = num
           print "%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s" % (match_bd.group(2), match_bd_name.group(2),match_profile_id.group(2),match_comm_id.group(2),match_office.rstrip('\n'),match_cusip.rstrip('\n'),match_bscd.strip(),match_accat.rstrip('\n'),match_soi.strip(),match_prd.strip(),match_actype.rstrip('\n'),linewd)
        if line.startswith('--------------------'):
           ReadMode = True


#print(max)

## Create a dictionary object to store formulla keys--Added in new logic
dict_keys = {'A':'% of Principal Value','B':'Surcharge, % of Current Commission','C':'$ Per share or Per Bond or Per Contract','D':'Flat Dollar Amount','E':'$ Per Round Lot','F':'% of Principal After Prior Level Range','G':'$ Per Share After Prior Level Range','H':'$ Per (next) Round Lot After Prior Level','J':'$ Per (current) Round Lot After Prior Level','N':'Non-Overriding Minimum (Apply Min; Check Pre Fig)','O':'Overriding Minimum (Apply Min, Check Pre-fig & Discount)','P':'Absolute Minimum (Apply Min; Stop)','S':'Sum Up to Commissions','T':'Table Values','W':'Absolute Maximum (Apply Max; Stop)','X':'Maximum (Apply Max; Check Pre-Fig & Discount)','Y':'Apply Discount; Check for Max and Pre-fig','Z':'Return to Pre-May Routine'}


#    print "%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s" % (match_bd.group(2), match_bd_name.group(2),match_profile_id.group(2),match_comm_id.group(2),match_office.rstrip('\n'),match_cusip.rstrip('\n'),match_bscd.strip(),match_accat.rstrip('\n'),match_soi.strip(),match_prd.strip(),match_actype.rstrip('\n'))
    ReadMode = False
    for line in str.splitlines():
        if ReadMode:
           sline = line.lstrip()
           linewd = re.sub(' +','|',sline)
           rw = linewd.split('|')
#          num = len(linewd) + 11
#           if max < num:
#              max = num
#           print "%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s" % (match_bd.group(2), match_bd_name.group(2),match_profile_id.group(2),match_comm_id.group(2),match_office.rstrip('\n'),match_cusip.rstrip('\n'),match_bscd.strip(),match_accat.rstrip('\n'),match_soi.strip(),match_prd.strip(),match_actype.rstrip('\n'),linewd) --comnted nw
           print "%s|%s|%s" % (rw[0],rw[1],rw[2])
        if line.startswith('--------------------'):
           ReadMode = True

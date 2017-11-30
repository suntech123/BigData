#!/usr/bin/python
import sys
import xml.etree.ElementTree as ET
context = ET.iterparse('/data6/splice/July_Files/DataWarehouse37/DataWarehouse37_FO_USA_M_201608034.xml',events=('end', ))
cnt=1
for event, elem in context:
    if elem.tag == 'InvestmentVehicle':
        title = 'sample'
        filename = format(title + ".xml")
        with open(filename, 'a') as f:
            if cnt == 1:
               f.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Package><PackageBody>")
            f.write(ET.tostring(elem))
            if cnt == 100:
               f.write("\n</PackageBody>\n</Package>")
               break
            cnt +=  1
sys.exit(0)

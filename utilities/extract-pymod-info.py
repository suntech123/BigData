##from bs4 import BeautifulSoup
data=[]
with open('C:\\Users\\KumarSU\\Desktop\\sun102.txt') as fd:
    str=fd.read()
##print(data)
##dt=re.findall(r'.*sys\..+?\w+',data)
##for i in dt:
##    print(i)
data=[s for s in str.splitlines()]
    
##print(sys.builtin_module_names)
##print(sys.modules)
for l in data:
    words=[]
    words=l.split()
    for x in words:
        if "os." in x:
            print(x)

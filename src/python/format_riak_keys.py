import re
import sys

key_pattern = re.compile('"([^"]+)"')
fin = open(sys.argv[1], 'r')
key_data = ''
for line in fin:
    key_data += line
fin.close()    
    
for m in key_pattern.finditer(key_data):
    k = m.group(1)
    if k != 'keys':
        print k
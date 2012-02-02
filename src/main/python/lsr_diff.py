# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import re
import sys

src_list = sys.argv[1]
dest_list = sys.argv[2]
print "SRC Cluster lsr results: " + src_list
print "DEST Cluster lsr results: " + dest_list

compare_dict = {}

def read_lsr_file(fname, is_dest):
    global compare_dict
    generic_field_grammar = "([^\s]+)"
    line_pattern = re.compile(r"%s\s+%s\s+%s\s+%s\s+%s\s+%s\s+%s\s+%s\s+" % (generic_field_grammar,generic_field_grammar,generic_field_grammar,generic_field_grammar,generic_field_grammar,generic_field_grammar,generic_field_grammar,generic_field_grammar))
    fin = open(fname, "r")
    try:
        for line in fin:
            m = line_pattern.search(line)
            isdir = 1 if m.group(1).startswith("d") else 0
            path = m.group(8)
            size = long(m.group(5))
            compare_dict.setdefault(path, [-1,-1, isdir])[is_dest] = size
    finally:
        fin.close()

read_lsr_file(src_list, 0)
read_lsr_file(dest_list, 1)

todelete = open("todelete.txt", "w")
tocopy = open("tocopy.txt", "w")
totalbytes = long(0)
try:
    for p,l in compare_dict.iteritems():
        # src missing (need to delete dest)
        if l[0] == -1:
            #print "SRC MISSING: %s" % (p)
            todelete.write(p)
            todelete.write('\n')

        # dest missing
        #if l[1] == -1:
            #print "DEST MISSING: %s" % (p)

        # dest out of sync with src (either not same size or dest missing - files only)
        if ((l[0] != l[1] and l[0] != -1) or l[1] == -1) and l[2] == 0:
            #print "OUT OF SYNC: %s" % (p)
            tocopy.write(p)
            tocopy.write('\n')
            totalbytes += l[0]
finally:        
    todelete.close()
    tocopy.close()
    
print "Need to copy %d bytes" % (totalbytes)
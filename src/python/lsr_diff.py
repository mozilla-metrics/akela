# ***** BEGIN LICENSE BLOCK *****
# Version: MPL 1.1/GPL 2.0/LGPL 2.1
#
# The contents of this file are subject to the Mozilla Public License Version
# 1.1 (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
# http://www.mozilla.org/MPL/
#
# Software distributed under the License is distributed on an "AS IS" basis,
# WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
# for the specific language governing rights and limitations under the
# License.
#
# The Original Code is __________________________________________.
#
# The Initial Developer of the Original Code is
# Mozilla Foundation.
# Portions created by the Initial Developer are Copyright (C) 2011
# the Initial Developer. All Rights Reserved.
#
# Contributor(s):
# Xavier Stevens <xstevens@mozilla.com>
# Alternatively, the contents of this file may be used under the terms of
# either the GNU General Public License Version 2 or later (the "GPL"), or
# the GNU Lesser General Public License Version 2.1 or later (the "LGPL"),
# in which case the provisions of the GPL or the LGPL are applicable instead
# of those above. If you wish to allow use of your version of this file only
# under the terms of either the GPL or the LGPL, and not to allow others to
# use your version of this file under the terms of the MPL, indicate your
# decision by deleting the provisions above and replace them with the notice
# and other provisions required by the GPL or the LGPL. If you do not delete
# the provisions above, a recipient may use your version of this file under
# the terms of any one of the MPL, the GPL or the LGPL.
#
# ***** END LICENSE BLOCK *****

import re
import sys

src_list = sys.argv[1]
dest_list = sys.argv[2]
print "SRC Cluster lsr results: " + src_list
print "DEST Cluster lsr results: " + dest_list

compare_dict = {}

def read_lsr_file(fname):
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
            compare_dict.setdefault(path, [-1,-1, isdir])[0] = size
    finally:
        fin.close()

read_lsr_file(src_list)
read_lsr_file(dest_list)

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
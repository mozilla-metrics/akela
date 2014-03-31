#!/usr/bin/env python
from __future__ import print_function
import os,sys
from config import config
from datetime import datetime
import glob

def construct_query(date):
    return """ \" SET SESSION AUTOCOMMIT TO ON; INSERT into f_bugs_status_resolution (bug_id,bug_severity,short_desc,bug_status,
       bug_status_previous,status_update,status_change_update,curr_snapshot_date)
    (SELECT bugs.bug_id AS bug_id,
           bugs.bug_severity AS bug_severity,
           bugs.short_desc AS short_desc,
           bugs.bug_status AS bug_status,
           prev_status.bug_status_prev AS bug_status_previous,
           CASE
             WHEN date_trunc ('day',modified_ts) = date_trunc ('day',snapshot_date) THEN bug_status
           END AS status_update,
           bugs.snapshot_date AS status_change_date,
           bugs.snapshot_date AS curr_snapshot_date
    FROM
    f_bugs_snapshot_v2 bugs
    LEFT JOIN (SELECT DISTINCT bug_status AS bug_status_prev,
                        bug_id,
                        bug_version_num,
                        snapshot_date AS prev_snapshot_date
                 FROM f_bugs_snapshot_v2) prev_status
             ON bugs.bug_id = prev_status.bug_id
            AND prev_status.prev_snapshot_date=bugs.snapshot_date-1
    WHERE 1=1
    AND   bug_status != bug_status_prev
    AND   date_trunc ('day',bugs.modified_ts) = date_trunc ('day',bugs.snapshot_date)
    AND bugs.snapshot_date='"""+ date +"""'
    ORDER BY bugs.snapshot_date, bug_id) \" """



def push_to_vertica():
    import sys
    date = None
    if len(sys.argv) == 2:
        date = sys.argv[1]
    if date == None:
        date = config['yesterday']

    try:
        query = construct_query(date)
        #auto_commit = "/opt/vertica/bin/vsql -c  \"SET SESSION AUTOCOMMIT TO ON\" -U "+ config['v_username'] + " -h "+ config['v_hostname'] + " -w "+ config['v_password']
        insert_to_db = "/opt/vertica/bin/vsql -c "+query + " -U "+ config['v_username'] + " -h "+ config['v_hostname'] + " -w "+ config['v_password']
        #print(insert_to_db, file = sys.stdout)
        #os.system(auto_commit)
        os.system(insert_to_db)
    except:
        print(sys.exc_info()[0],file=sys.stdout)
        raise

if __name__ == "__main__":
    push_to_vertica()

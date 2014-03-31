#!/usr/bin/env python
import os,sys,subprocess
from datetime import datetime,date,timedelta as td

def backfill():
    import sys
    try:
        start_date = datetime.strptime(sys.argv[1],'%Y-%m-%d')
        end_date = datetime.strptime(sys.argv[2],'%Y-%m-%d')
        delta = end_date - start_date
        count = 1
        for i in range(delta.days + 1):
            curr_date= (start_date + td(days=i)).strftime('%Y-%m-%d')
            cmd ="/home/schintalapani/bugzilla_snapshot/fetch_es_bugzilla.py "+curr_date
            print(cmd)
            os.system(cmd)
            cmd = "/home/schintalapani/bugzilla_snapshot/bug_status_resolution_vertica.py "+curr_date
            print(cmd)
            os.system(cmd)
    except:
        print(sys.exc_info()[0])
        raise

if __name__ == "__main__":
    backfill()

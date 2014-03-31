#!/usr/bin/env python
from __future__ import print_function
import requests
import json,sys,os,errno
from config import config
from datetime import datetime,date,timedelta as td
import time,glob

def fetch_bugzilla_data():
    offset = read_offset(config['offset_file'])
    total = 977595 # best estimate

    if offset == None or offset == "":
        raise "There is no offset in the file"
    
    while int(offset) <= total:
        query=construct_query(offset,config['size'])
        try:
            results = fetch_es_data(query)
            parse_bugzilla_data(results,offset)
            total = config['total']
            new_offset = int(offset) + int(config['size'])
            commit_offset(config['offset_file'],str(new_offset))
        except:
            #print(results,file=sys.stdout)
            print(sys.exc_info()[0],file=sys.stdout)
            raise
        time.sleep(config['delay'])
        offset = read_offset(config['offset_file'])


def fetch_bugzilla_count():
    try:
        query=total_count_query()
        results=fetch_es_data(query)
        config['total'] = results['hits']['total']
    except:
        print(sys.exc_info()[0],file=sys.stdout)
        raise

def merge_files():
    merge_file = config['merge_file']
    f = open(merge_file, 'w')
    for file in glob.glob(config['tmp_dir']+"/bugzilla_data_*"):
        f.write(open(file).read())
    f.close()

def remove_dups():
    merge_file = config['merge_file']
    uniq_file = config['merge_file']+'_uniq'
    try:
        config['merge_file_uniq'] = uniq_file
        #os_remove_dups = 'sort '+config['merge_file']+' | uniq > '+config['merge_file_uniq']
        os_remove_dups = "awk '!x[$1,$4]++' FS=\",\" "+ config['merge_file']+" > " + config['merge_file_uniq']
        #print(os_remove_dups,file=sys.stdout)
        os.system(os_remove_dups)
    except:
        print(sys.exc_info()[0],file=sys.stdout)
        raise

def read_offset(offset_file):
    with open(offset_file, 'r') as f:
        first_line = f.readline()
        return str(int(first_line))


def commit_offset(offset_file,offset):
    f = open(offset_file,'w')
    print(offset,file=f)


def convert_value(val):
    if type(val) == unicode:
        return val.encode('utf-8')
    else:
        return str(val).encode('utf-8')

def push_to_vertica():
    merge_file = config['merge_file_uniq']
    try:
        insert_to_db = "/opt/vertica/bin/vsql -c \"copy " + config['v_table'] + " from LOCAL '" + merge_file + "' delimiter ',' rejected data '" +merge_file+"_rejected.txt' exceptions '"+merge_file+"_exceptions.txt';\" -U " + config['v_username'] + " -h " + config['v_hostname']+ " -w " + config['v_password']
        os.system(insert_to_db)
    except:
        print(sys.exc_info()[0],file=sys.stdout)
        raise

def parse_bugzilla_data(results,offset):
    f = open(config['tmp_dir']+"/bugzilla_data_"+offset,"w")
    for hit in results['hits']['hits']:
        fields = hit['fields']
        line = []
        line.append(fields['bug_id'])
        line.append(fields['bug_severity']) if 'bug_severity' in fields else line.append('')
        line.append(fields['bug_status']) if 'bug_status' in fields else line.append('')
        line.append(fields['bug_version_num']) if 'bug_version_num' in fields else line.append('')
        line.append(fields['assigned_to'].replace(",","|")) if 'assigned_to' in fields else line.append('')
        line.append(fields['component'].replace(",","|")) if 'component' in fields else line.append('')
        line.append(fields['created_by'].replace(",","|")) if 'created_by' in fields else line.append('')
        line.append(fields['created_ts']) if 'created_ts' in fields else line.append('')
        line.append(fields['modified_by'].replace(",","|")) if 'modified_by' in fields else line.append('')
        line.append(fields['modified_ts']) if 'modified_ts' in fields else line.append('')
        line.append(fields['op_sys'].replace(",","|")) if 'op_sys' in fields else line.append('')
        line.append(fields['priority'].replace(",","|")) if 'priority' in fields else line.append('')
        line.append(fields['product'].replace(",","|")) if 'product' in fields else line.append('')
        line.append(fields['qa_contact'].replace(",","|")) if 'qa_contact' in fields else line.append('')
        line.append(fields['reported_by'].replace(",","|")) if 'reported_by' in fields else line.append('')
        line.append(fields['reporter'].replace(",","|")) if 'reporter' in fields else line.append('')
        line.append(fields['version'].replace(",","|")) if 'version' in fields else line.append('')
        line.append(fields['expires_on']) if 'expires_on' in fields else line.append('')
        line.append(fields['cf_due_date']) if 'cf_due_date' in fields else line.append('')
        line.append(fields['target_milestone']) if 'target_milestone' in fields else line.append('')
        line.append('|'.join(fields["keywords"])) if 'keywords' in fields else line.append('')
        line.append(fields['short_desc']) if 'short_desc' in fields else line.append('')
        line.append(fields['resolution']) if 'resolution' in fields else line.append('')
        line.append(config['yesterday'])
        try:
            print(','.join(map(convert_value,line)),file=f)
        except:
            print(results,file=sys.stdout)
            print(sys.exc_info()[0],file=sys.stdout)
            raise



def fetch_es_data(query):
    uri=config['es_host']
    r = requests.get(uri,data=query)
    results = json.loads(r.text)
    return results


def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else: raise

def init_config():
    import sys
    date = None
    if len(sys.argv) == 2:
        date = sys.argv[1]
    if date != None:
        config['yesterday'] = date
    config['merge_file'] = "bugs_snapshot_"+config['yesterday']
    config['tmp_dir'] = "tmp_data_"+config['yesterday']
    mkdir_p(config['tmp_dir'])
    config['offset_file'] = 'es_offset_'+config['yesterday']
    offset_file = open(config['offset_file'],'w')
    print("0",file=offset_file)

def cleanup():
    import shutil
    try:
        os.remove(config['merge_file'])
        os.remove(config['merge_file_uniq'])
        shutil.rmtree(config['tmp_dir'])
    except OSError as exc:
        raise


def total_count_query():
    yesterday = config['yesterday']+" 23:59:59.999"
    #yesterday = config['yesterday']+"T23:59:59.999Z"
    pattern = '%Y-%m-%d %H:%M:%S.%f'
    epoch_ts = str(int(time.mktime(time.strptime(yesterday,pattern))) * 1000)
    offset = "0"
    limit = "10"
    query = """{"query": {"filtered": { "query": { "match_all": {}},"filter": {"bool": {"must" :[ {"range": {"modified_ts": {"lt": \""""+epoch_ts+"""\"}}},
    {"range": {"expires_on": {"gt": \""""+epoch_ts+"""\"}}}]}}}},"from": """+offset+""","size": """+limit+""", "sort": [{"bug_id":{}}],
     "fields": ["assigned_to","bug_id","bug_severity","bug_status","bug_version_num","component",
     "created_by","created_ts","modified_by","modified_ts","op_sys","priority","product","qa_contact","reported_by","reporter",
     "version","expires_on","keywords","cf_due_date","target_milestone","short_desc","resolution"]}"""    
    #epoch_ts = yesterday
    return query


def construct_query(offset,limit):
    yesterday = config['yesterday']+" 23:59:59.999"
    #yesterday = config['yesterday']+"T23:59:59.999Z"
    pattern = '%Y-%m-%d %H:%M:%S.%f'
    epoch_ts = str(int(time.mktime(time.strptime(yesterday,pattern))) * 1000)
    to = str(int(offset)+int(limit))
    query = """{"query": {"filtered": { "query": { "match_all": {}},"filter": {"and": [{"range": {"bug_id" : {"gte":\""""+offset+"""\",
     "lt": \""""+to+"""\"}}},{"range": {"modified_ts": {"lt": \""""+epoch_ts+"""\"}}},{"range": {"expires_on": {"gt": \""""+epoch_ts+"""\"}}}]}}},
     "size":"""+ limit+""","fields": ["assigned_to","bug_id","bug_severity","bug_status","bug_version_num","component",
     "created_by","created_ts","modified_by","modified_ts","op_sys","priority","product","qa_contact","reported_by","reporter",
     "version","expires_on","keywords","cf_due_date","target_milestone","short_desc","resolution"]}"""
    return query


if __name__ == "__main__":
    init_config()
    fetch_bugzilla_count()
    fetch_bugzilla_data()
    merge_files()
    remove_dups()
    push_to_vertica()
    cleanup()

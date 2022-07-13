import civis
import os
import pandas as pd
import time
import requests
import numpy as np

api_key = ''

def create_civis_df(sql):
    try:
        df = civis.io.read_civis_sql(sql, database = 'CWA', use_pandas = True)
        time.sleep(10)
        print('Df created.')
    except civis.base.EmptyResultError:
        print('EmptyResultError.')
        df = pd.DataFrame()
        pass

    return df

sql = ''' 
with current_file as (select distinct file from org_lumen.barg order by file desc limit 1),
last_file as (select distinct file from org_lumen.barg where file <> (select * from current_file) order by file desc limit 1)
select distinct concat('Lumen-', barg.empid) as empid, w.id from org_lumen.barg 
join org_lumen.ab_worklocations w
on upper(concat(barg.workstreet, concat(', ', concat(barg.workcity, concat(', ', barg.workstate_acronym))))) = upper(w.name)
where file = (select * from last_file)
and unionname = 'CWA' except
select distinct concat('Lumen-', barg.empid) as empid, w.id from org_lumen.barg 
join org_lumen.ab_worklocations w
on upper(concat(barg.workstreet, concat(', ', concat(barg.workcity, concat(', ', barg.workstate_acronym))))) = upper(w.name)
where file = (select * from current_file) and unionname = 'CWA' '''

df = create_civis_df(sql)

print(df)
df = df.fillna('')
df_dict = df.set_index('empid').T.to_dict('dict')

campaign_id = ''
url = 'https://cwatest.actionbuilder.org/api/rest/v1/campaigns/' + campaign_id + '/people/'
headers = {'OSDI-API-Token': api_key, 'Content-Type': 'application/json'}

error_list = []
count = 0
no_conn = 0

for key in df_dict:
    print(key)
    filter = url + '''?filter=identifier eq 'custom_id_1:{}' '''.format(key)
    r = requests.get(url=filter, headers=headers)
    person = r.json()
    ab_id = person['_embedded']['osdi:people'][0]['identifiers'][0][15:]
    print(ab_id)
    person_conn_url = person['_embedded']['osdi:people'][0]['_links']['action_builder:connections']['href']

    r = requests.get(url=person_conn_url, headers=headers)
    connection = r.json()
    
    filter = url + '''?filter=identifier eq 'custom_id_4:{}' '''.format(df_dict[key]['id'])
    r = requests.get(url=filter, headers=headers)
    workloc = r.json()
    work_loc_ab_id = workloc['_embedded']['osdi:people'][0]['identifiers'][0][15:]
    print(work_loc_ab_id)

    try:
        num = len(connection['_embedded']['action_builder:connections'])

        for i in range(num):
            if connection['_embedded']['action_builder:connections'][i]['person_id'] != work_loc_ab_id:
                pass
            else:
                conn_url = connection['_embedded']['action_builder:connections'][i]['_links']['self']['href']
            
                data = {"inactive": True}

                r = requests.put(json=data, url=conn_url, headers=headers)

                if r.status_code == 201 or r.status_code == 200:
                    count += 1
                else:
                    error_list.append({key: r.status_code})
    except:
        print('No connections found')
        no_conn += 1

print(str(count) + ' records updated.')
print(str(no_conn) + ' connections not found.')
print(error_list)

# output variable for summary results
import json

def post_json_run_output(json_value_dict):
    client = civis.APIClient()
    json_value_object = client.json_values.post(
        json.dumps(json_value_dict),
        name='email_outputs')
    client.scripts.post_containers_runs_outputs(
        os.environ['CIVIS_JOB_ID'],
        os.environ['CIVIS_RUN_ID'],
        'JSONValue',
        json_value_object.id)

number = len(df)

json_value_dict = {
    'number': number,
    'count': count,
    'error_list': error_list
}
post_json_run_output(json_value_dict)

print(json_value_dict)
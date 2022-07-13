import civis
import os
import pandas as pd
import time
import requests

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


sql_worklocation_info_reactivate = ''' 
with current_file as (select distinct file from org_lumen.barg order by file desc limit 1)
select distinct w.id, w.name, 'D7' as district, s.unionlocal
from org_lumen.barg s
join org_lumen.ab_worklocations w on concat(s.workstreet, concat(', ', concat(s.workcity, concat(', ', s.workstate_acronym)))) = upper(w.name)
WHERE file = (select * from current_file) and unionname = 'CWA' and w.status = 'Inactive'  '''

df = create_civis_df(sql_worklocation_info_reactivate)
error_list = []
count = 0

if df.empty:
    print('Empty df.')
else:
    print(df)
    df_dict = df.set_index('id').T.to_dict('list')

    campaign_id = ''
    url = 'https://cwatest.actionbuilder.org/api/rest/v1/campaigns/' + campaign_id + '/people/'
    headers = {'OSDI-API-Token': api_key, 'Content-Type': 'application/json'}

    for key in df_dict:
        data = {
            "person": {
                "identifiers": ["custom_id_4:" + key],
                "action_builder:entity_type": "Work Location"
            },
            "add_tags": [{
            "action_builder:section": "Work Location Info",
            "action_builder:field": "CWA District",
            "action_builder:name": df_dict[key][0]},
            {"action_builder:section": "Work Location Info",
            "action_builder:field": "CWA Local",
            "action_builder:name": df_dict[key][1]}]}

        r = requests.post(json=data, url=url, headers=headers)
        print(r.text)
        print(r.status_code)

        if r.status_code == 201 or r.status_code == 200:
            count += 1
        else:
            error_list.append({key: r.status_code})

    update_table = ''' update org_lumen.ab_worklocations 
                    set status = 'Active' 
                    where id in (with current_file as (select distinct file from org_lumen.barg order by file desc limit 1)
                    select distinct w.id
                    from org_lumen.barg s
                    join org_lumen.ab_worklocations w on concat(s.workstreet, concat(', ', concat(s.workcity, concat(', ', s.workstate_acronym)))) = upper(w.name)
                    WHERE file = (select * from current_file) and unionname = 'CWA' and w.status = 'Inactive') '''

    run = civis.io.query_civis(sql = update_table, database = 'CWA')
    run.result()

print(str(count) + ' records updated.')
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
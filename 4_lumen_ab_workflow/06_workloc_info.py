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

sql_worklocation_info = ''' with current_file as (select distinct file from org_lumen.barg order by file desc limit 1)
select distinct w.id, unionlocal, workstreet, workcity, workstate_acronym from org_lumen.barg
join org_lumen.ab_worklocations w on concat(workstreet, concat(', ', concat(workcity, concat(', ', workstate_acronym)))) = upper(w.name)
WHERE file = (select * from current_file) and unionname = 'CWA' and workstreet != 'WORKS FROM HOME'
except
select distinct w.id, unionlocal, workstreet, workcity, workstate_acronym from org_lumen.barg
join org_lumen.ab_worklocations w on concat(workstreet, concat(', ', concat(workcity, concat(', ', workstate_acronym)))) = upper(w.name)
WHERE file <> (select * from current_file) and unionname = 'CWA' and workstreet != 'WORKS FROM HOME' '''

df = create_civis_df(sql_worklocation_info)
error_list = []
count = 0

if df.empty:
    print('Empty dataframe.')
else:
    df['unionlocal'] = df['unionlocal'].astype(str)
    print(df)
    df_dict = df.set_index('id').T.to_dict('dict')

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
            "action_builder:name": 'D7'},
            {"action_builder:section": "Work Location Info",
            "action_builder:field": "CWA Local",
            "action_builder:name": df_dict[key]['unionlocal']}]}

        r = requests.post(json=data, url=url, headers=headers)
        print(r.text)
        print(r.status_code)
        
        if r.status_code == 201 or r.status_code == 200:
            count += 1
        else:
            error_list.append({key: r.status_code})

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
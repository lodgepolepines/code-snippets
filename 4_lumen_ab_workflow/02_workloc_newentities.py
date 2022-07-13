import civis
import os
import pandas as pd
import re
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


def upper_to_proper_case(s):
    """
    Takes an all-caps string s and lowercases all non-leading characters, while
    preserving cardinal directions and caps PO boxes.
    """
    new_string = re.sub('(\S)([A-Z]+)', lambda x : x.group(1)+x.group(2).lower(), s)

    cardinal_dict = {'Se ': 'SE ', 'Ne ': 'NE ', 'Sw ': 'SW ', 'Nw ': 'NW ',
    ' Se ': ' SE ', ' Ne ': ' NE ', ' Sw ': ' SW ', ' Nw ': ' NW ',
    ' Se': ' SE', ' Ne': ' NE', ' Sw': ' SW', ' Nw': ' NW'}

    start_cardinal_list = ['Se ', 'Ne ', 'Sw ', 'Nw ']
    for i in start_cardinal_list:
        new_string = re.sub('^' + i, cardinal_dict[i], new_string)

    mid_cardinal_list = [' Se ', ' Ne ', ' Sw ', ' Nw ']
    for i in mid_cardinal_list:
        new_string = re.sub(i, cardinal_dict[i], new_string)

    end_cardinal_list = [' Se', ' Ne', ' Sw', ' Nw']
    for i in end_cardinal_list:
        new_string = re.sub(i + '$', cardinal_dict[i], new_string)

    other_list = ['Po Box', 'P.o. Box', 'At&t']
    other_list_dict = {'Po Box': 'PO Box', 'P.o. Box': 'P.O. Box', 'At&t': 'AT&T'}
    for i in other_list:
        new_string = re.sub('^' + i, other_list_dict[i], new_string)
    
    new_string = re.sub(' At&t ', 'AT&T', new_string)

    return new_string


sql_work_locations = ''' 
with current_file as (select distinct file from org_lumen.barg order by file desc limit 1)
SELECT distinct concat(workstreet, concat(', ', concat(workcity, concat(', ', workstate_acronym)))) as name,
workstreet as address, workcity as city, workstate_acronym as state
FROM org_lumen.barg
WHERE file = (select * from current_file) and 
name NOT IN
    (SELECT upper(name) 
     FROM org_lumen.ab_worklocations) and workstreet is not null and workcity is not null and workstate is not null '''

df = create_civis_df(sql_work_locations)

if df.empty:
    print('Empty df.')
else:
    df = df.dropna(subset=['address'])

    df['name'] = df['name'].astype(str).apply(upper_to_proper_case)
    df['address'] = df['address'].astype(str).apply(upper_to_proper_case)
    df['city'] = df['city'].astype(str).apply(upper_to_proper_case)
    df['new_name'] = df['address'] + ', ' + df['city'] + ', ' + df['state']
    df['name'] = df['new_name']
    df_len = len(df)

    sql = '''SELECT RIGHT(id, LEN(id) - 13) AS worklocid from org_lumen.ab_worklocations'''
    workloc_df = create_civis_df(sql)
    max_id = workloc_df["worklocid"].max()

    # get max_id + 1, and length of new df + 1
    id_list = []
    for i in range(max_id + 1, max_id + df_len + 1):
        id_list.append('lumen_workloc' + str(i))

    print(id_list)
    df['id'] = id_list
    df = df.reindex(columns=["id", "name", "address", "city", "state"])
    df['status'] = 'Active'
    df = df.fillna('')
    print(df)
    df_dict = df.set_index('id').T.to_dict('list')

    campaign_id = ''
    url = 'https://cwatest.actionbuilder.org/api/rest/v1/campaigns/' + campaign_id + '/people/'
    headers = {'OSDI-API-Token': api_key, 'Content-Type': 'application/json'}

    for key in df_dict:
        data = {
            "person": {
                "identifiers": ["custom_id_4:" + key],
                "action_builder:entity_type": "Work Location",
                "action_builder:name" : df_dict[key][0],
                    "postal_addresses": [{"address_lines": df_dict[key][1],
                    "locality": df_dict[key][2],
                    "region": df_dict[key][3],
                    "country": "US",
                    "status": "verified"}]
            }
            }

        r = requests.post(json=data, url=url, headers=headers)
        print(r.text)

    df_append = civis.io.dataframe_to_civis(df,
                                database='CWA',
                                table='org_lumen.ab_worklocations',
                                existing_table_rows='append')
    print(df_append.result())

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
    'number': number
}
post_json_run_output(json_value_dict)

print(json_value_dict)
import civis
import os
import pandas as pd
import re
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
    
    new_string = re.sub(' At&t ', ' AT&T ', new_string)

    return new_string
    

# new / changed records
sql = ''' with current_file as (select distinct file from org_lumen.barg order by file desc limit 1)
select distinct concat('Lumen-', barg.empid) as empid, barg.firstname, barg.lastname,
barg.unionlocal, 'D7' as district, CASE WHEN barg.workstreet != 'WORKS FROM HOME' THEN
w.name ELSE 'Works From Home' end as worklocation, barg.workstreet, barg.workcity, barg.workstate,
barg.company, barg.worktourtype, barg.servicedate, case when barg.workstreet = 'WORKS FROM HOME' then 'Works From Home'
else NULL end as wfh_status, barg.workstate_acronym
from org_lumen.barg
join org_lumen.ab_worklocations w
on upper(concat(barg.workstreet, concat(', ', concat(barg.workcity, concat(', ', barg.workstate_acronym))))) = upper(w.name)
where file = (select * from current_file) and unionname = 'CWA'
except
select distinct concat('Lumen-', barg.empid) as empid, barg.firstname, barg.lastname,
barg.unionlocal, 'D7' as district, CASE WHEN barg.workstreet != 'WORKS FROM HOME' THEN
w.name ELSE 'Works From Home' end as worklocation, barg.workstreet, barg.workcity, barg.workstate,
barg.company, barg.worktourtype, barg.servicedate, case when barg.workstreet = 'WORKS FROM HOME' then 'Works From Home'
else NULL end as wfh_status, barg.workstate_acronym
from org_lumen.barg 
join org_lumen.ab_worklocations w
on upper(concat(barg.workstreet, concat(', ', concat(barg.workcity, concat(', ', barg.workstate_acronym))))) = upper(w.name)
where file = (select distinct file from org_lumen.barg where file <> (select * from current_file) order by file desc limit 1) 
and unionname = 'CWA' '''

df = create_civis_df(sql)

sql_number = ''' select distinct file from org_lumen.dues order by file desc limit 1 '''
num_df = create_civis_df(sql_number)
file_num = num_df['file'][0][:4]

sql2 = '''
SELECT DISTINCT concat('Lumen-', dues.empid) as empid, firstname, lastname, local,
case when local = '13000' then 'D2-13'
when local ilike '1%' then 'D1'
when local ilike '2%' then 'D2-13'
when local ilike '3%' then 'D3'
when local ilike '4%' then 'D4'
when local ilike '6%' then 'D6'
when local ilike '7%' then 'D7'
when local ilike '9%' then 'D9'
END as district
FROM org_lumen.dues
WHERE file ILIKE '{}%'
AND NOT EXISTS (SELECT * from org_lumen.barg WHERE barg.empid = dues.empid
and barg.file ILIKE '{}%' and barg.unionname = 'CWA') '''.format(file_num, file_num)

df2 = create_civis_df(sql2)

df = df.fillna('')
df.firstname = df.firstname.str.title()
df.lastname = df.lastname.str.title()
df.unionlocal = df.unionlocal.astype(str)
df.empid = df.empid.astype(str)
df_dict = df.set_index('empid').T.to_dict('list')
df['workstreet'] = df['workstreet'].apply(upper_to_proper_case)
df['workcity'] = df['workcity'].apply(upper_to_proper_case)
df['worklocation'] = np.where(df['workstreet'].str.contains('Works From Home'), df['worklocation'] + ' - ' + df['workstate_acronym'], df['worklocation'])

df2 = df2.fillna('')
df2.firstname = df2.firstname.str.title()
df2.lastname = df2.lastname.str.title()
df2.local = df2.local.astype(str)
df2.empid = df2.empid.astype(str)
df_dict2 = df2.set_index('empid').T.to_dict('list')

campaign_id = ''
url = 'https://cwatest.actionbuilder.org/api/rest/v1/campaigns/' + campaign_id + '/people/'
headers = {'OSDI-API-Token': api_key, 'Content-Type': 'application/json'}

error_list = []
count = 0

for key in df_dict:
    data = {
        "person": {
            "identifiers": ["custom_id_1:" + key],
            "action_builder:entity_type": "Person",
            "given_name": df_dict[key][0],
            "family_name": df_dict[key][1]
            
        },
        "add_tags": [
            {
            "action_builder:section": "Job Info [Lumen]",
            "action_builder:field": "CWA Local",
            "action_builder:name": df_dict[key][2]},
            {
            "action_builder:section": "Job Info [Lumen]",
            "action_builder:field": "CWA District",
            "action_builder:name": df_dict[key][3]},
            {
            "action_builder:section": "Job Info [Lumen]",
            "action_builder:field": "Work Location",
            "action_builder:name": df_dict[key][4]},
            {
            "action_builder:section": "Job Info [Lumen]",
            "action_builder:field": "Company Code",
            "action_builder:name": df_dict[key][8]},
            {
            "action_builder:section": "Job Info [Lumen]",
            "action_builder:field": "Work Tour Type",
            "action_builder:name": df_dict[key][9]},
            {
            "action_builder:section": "Job Info [Lumen]",
            "action_builder:field": "NCS Date",
            "action_builder:name": "NCS Date",
            "action_builder:date_response": df_dict[key][10]},
            {
            "action_builder:section": "Job Info [Lumen]",
            "action_builder:field": "WFH Status",
            "action_builder:name": df_dict[key][11]},
            {
            "action_builder:section": "Job Info [Lumen]",
            "action_builder:field": "Work State",
            "action_builder:name": df_dict[key][12]}
        ]}

    r = requests.post(json=data, url=url, headers=headers)
    print(r.text)
    print(r.status_code)

    if r.status_code == 201 or r.status_code == 200:
        count += 1
    else:
        error_list.append({key: r.status_code})

print(str(count) + ' records updated.')
print(error_list)

error_list2 = []
count2 = 0

for key in df_dict2:
    data = {
        "person": {
            "identifiers": ["custom_id_1:" + key],
            "action_builder:entity_type": "Person",
            "given_name": df_dict2[key][0],
            "family_name": df_dict2[key][1]
            
        },
        "add_tags": [
            {
            "action_builder:section": "Job Info [Lumen]",
            "action_builder:field": "CWA Local",
            "action_builder:name": df_dict2[key][2]},
            {
            "action_builder:section": "Job Info [Lumen]",
            "action_builder:field": "CWA District",
            "action_builder:name": df_dict2[key][3]}
        ]}

    r = requests.post(json=data, url=url, headers=headers)
    print(r.text)
    print(r.status_code)

    if r.status_code == 201 or r.status_code == 200:
        count2 += 1
    else:
        error_list2.append({key: r.status_code})

print(str(count2) + ' records updated.')
print(error_list2)

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

number = len(df) + len(df2)

json_value_dict = {
    'number': number,
    'count': count,
    'error_list': error_list,
    'count2': count2,
    'error_list2': error_list2
}
post_json_run_output(json_value_dict)

print(json_value_dict)
import civis
import os
import pandas as pd
import time
import requests
import re

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


sql = ''' 
with current_file as (select distinct file from org_lumen.barg order by file desc limit 1)
select distinct concat('Lumen-', barg.empid) as empid, firstname, lastname, homestreet, homecity, workstate_acronym as state, homezip
from org_lumen.barg
where file = (select * from current_file) and unionname = 'CWA'
except
select distinct concat('Lumen-', barg.empid) as empid, firstname, lastname, homestreet, homecity, workstate_acronym as state, homezip
from org_lumen.barg 
where file = (select distinct file from org_lumen.barg where file <> (select * from current_file) order by file desc limit 1) 
and unionname = 'CWA' '''

df = create_civis_df(sql)
df['homestreet'] = df['homestreet'].astype(str)
df['homecity'] = df['homecity'].astype(str)
df['homestreet'] = df['homestreet'].apply(upper_to_proper_case)
df['homecity'] = df['homecity'].apply(upper_to_proper_case)

df = df.dropna(subset=['homestreet', 'homecity', 'state'])
df['homezip'] = df['homezip'].astype(int)
df['homezip'] = df['homezip'].astype(str)
df['homezip'] = df['homezip'].str.zfill(5)
df.firstname = df.firstname.str.title()
df.lastname = df.lastname.str.title()
df.empid = df.empid.astype(str)
df = df.fillna('')
df_dict = df.set_index('empid').T.to_dict('dict')

campaign_id = ''
url = 'https://cwatest.actionbuilder.org/api/rest/v1/campaigns/' + campaign_id + '/people/'
headers = {'OSDI-API-Token': api_key, 'Content-Type': 'application/json'}

# search table for existing address + name id sourced from barg file
# if no match is found add as new without ab identifier
# else match on ab identifier and replace old address

sql_get_address_interact_id = ''' select interact_id, custom_id_1 from org_lumen.ab_addresses_barg'''
address_df = create_civis_df(sql_get_address_interact_id)
addr_dict = address_df.set_index('custom_id_1').T.to_dict('dict')

col = ['interact_id', 'custom_id_1']
new_df = pd.DataFrame(columns = col)

error_list = []
count = 0
replacements = 0

for key in df_dict:
    if key in addr_dict:
        # add using addr interact id
        print('key in dict')

        data = {
        "person": {
            "identifiers": ["custom_id_1:" + key],
            "action_builder:entity_type": "Person",
            "postal_addresses": [
          {
            "action_builder:identifier": addr_dict[key]['interact_id'],
            "address_lines": [
              df_dict[key]['homestreet']
            ],
            "locality": df_dict[key]['homecity'],
            "region": df_dict[key]['state'],
            "postal_code": df_dict[key]['homezip'],
            "country": "US"
          }
        ]
        }}

        r = requests.post(json=data, url=url, headers=headers)
        print(r.text)

        if r.status_code == 201 or r.status_code == 200:
            count += 1
            replacements += 1
        else:
            error_list.append({key: r.status_code})
        
    else:
        print('key not in dict')
        data = {
        "person": {
            "identifiers": ["custom_id_1:" + key],
            "action_builder:entity_type": "Person",
            "given_name": df_dict[key]['firstname'],
            "family_name": df_dict[key]['lastname'],
            "postal_addresses": [
          {
            "address_lines": [
              df_dict[key]['homestreet']
            ],
            "locality": df_dict[key]['homecity'],
            "region": df_dict[key]['state'],
            "postal_code": df_dict[key]['homezip'],
            "country": "US"
          }
        ]
        }}

        r = requests.post(json=data, url=url, headers=headers)
        print(r.text)

        if r.status_code == 201 or r.status_code == 200:
            count += 1
        else:
            error_list.append({key: r.status_code})

        # get new addr interact id, append to df
        person_dict = r.json()
        addr_id = person_dict['postal_addresses'][0]['action_builder:identifier'][15:]
        row = [addr_id, key]
        append_df = pd.DataFrame([row], columns = col)
        new_df = pd.concat([new_df, append_df], ignore_index = True)

print(str(count) + ' records updated.')
print(str(replacements) + ' replacements updated.')
print(error_list)  

print(new_df)
# insert df of new addr interact ids to table
print('Appending address df to Civis table...')
table_append = civis.io.dataframe_to_civis(new_df,
                            database='CWA',
                             table='org_lumen.ab_addresses_barg',
                             existing_table_rows='append')
print(table_append.result())

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
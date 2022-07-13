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

aptify_sql = ''' 
with current_file as (select distinct file from org_lumen.barg order by file desc limit 1)
select distinct concat('Lumen-', barg.empid) as empid, t.id, barg.firstname, barg.lastname, t.statusname
from org_lumen.barg
join vwTim_Org_Hash t
on barg.empid = t.clockid
where file = (select * from current_file)
and unionname = 'CWA' and (t.statusname ilike '%- Active -%' or t.statusname ilike '%On Leave%')
and t.processingunitnumber in ('0001702',
'0001703',
'0002901',
'0003401',
'0008301',
'0012101',
'0013201',
'0019901',
'0028301',
'0041803',
'0059901',
'0060401',
'0062901',
'0071001',
'0072102',
'0081401',
'0123501',
'0134401',
'0186601',
'0237801',
'0645301',
'0685901',
'0760901',
'0772601') '''

aptify_df = create_civis_df(aptify_sql)

new = aptify_df["statusname"].str.split('-', n = 2, expand = True)
aptify_df["member_status"]= new[0]
aptify_df['member_status'] = aptify_df['member_status'].str.strip()
aptify_df = aptify_df[['id', 'firstname', 'lastname', 'statusname', 'member_status', 'empid']]

# create new refresh table
print('Deleting previous refresh Aptify table.')
drop_sql = ''' DROP TABLE org_lumen.aptify_refresh '''
drop_table = civis.io.query_civis(drop_sql, database = 'CWA')
print(drop_table.result())

time.sleep(5)

print('Creating refresh Aptify table...')
aptify_table_sql = '''CREATE TABLE IF NOT EXISTS org_lumen.aptify_refresh
    (id varchar(65535),
    firstname varchar(65535),
    lastname varchar(65535),
    statusname varchar(65535),
    member_status varchar(65535),
    empid varchar(65535));'''

create_table = civis.io.query_civis(aptify_table_sql, database = 'CWA')
print(create_table.result())

time.sleep(5)

print('Transferring df to Civis table...')
transfer_aptify = civis.io.dataframe_to_civis(aptify_df, 'CWA', 'org_lumen.aptify_refresh')
print(transfer_aptify.result())

print('Creating sql queries for API updates...')

sql_refresh = '''
SELECT DISTINCT empid, id as custom_id_3, firstname, lastname, statusname as aptify_status
FROM org_lumen.aptify_refresh
WHERE NOT EXISTS (SELECT 1 FROM org_lumen.aptify_temp WHERE aptify_temp.empid = aptify_refresh.empid AND aptify_temp.statusname = aptify_refresh.statusname) '''

df = create_civis_df(sql_refresh)
print(df)

campaign_id = ''
url = 'https://cwatest.actionbuilder.org/api/rest/v1/campaigns/' + campaign_id + '/people/'
headers = {'OSDI-API-Token': api_key, 'Content-Type': 'application/json'}

count = 0
error_list = []
processed = []

if df.empty:
    pass
else:
    df = df.fillna('')
    df['custom_id_3'] = df['custom_id_3'].astype(str)
    df_dict = df.set_index('empid').T.to_dict('dict')

    for key in df_dict:

        data = {
            "person": {
                "identifiers": ["custom_id_3:" + df_dict[key]['custom_id_3'], "custom_id_1:" + key],
                "action_builder:entity_type": "Person"
            },
            "add_tags": [{
            "action_builder:section": "Member Info",
            "action_builder:field": "Aptify Status",
            "action_builder:name": df_dict[key]['aptify_status']}]}

        r = requests.post(json=data, url=url, headers=headers)

        if r.status_code == 201 or r.status_code == 200:
            count += 1
            processed.append(key)
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
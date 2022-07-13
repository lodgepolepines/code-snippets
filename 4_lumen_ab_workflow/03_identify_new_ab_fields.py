import civis
import os
import pandas as pd
import time
import json

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


sql = ''' 
with current_file as (select distinct file from org_lumen.barg order by file desc limit 1)
select distinct concat(workstreet, concat(', ', concat(workcity, concat(', ', workstate_acronym)))) as name, 'Work Location' as field 
from org_lumen.barg
where file = (select * from current_file) and unionname = 'CWA'
except
select distinct concat(workstreet, concat(', ', concat(workcity, concat(', ', workstate_acronym)))) as name, 'Work Location' as field 
from org_lumen.barg
where file != (select * from current_file) and unionname = 'CWA'
union 
select distinct unionname as name, 'CWA Local' as field 
from org_lumen.barg
where file = (select * from current_file) and unionname = 'CWA'
except
select distinct unionname as name, 'CWA Local' as field 
from org_lumen.barg
where file != (select * from current_file) and unionname = 'CWA' '''

df = create_civis_df(sql)

if df.empty:
    print('Empty df.')
    cwa_locals = []
    work_locations = []
else:
    cwa_locals = df.loc[df['field'] == 'CWA Local', ['name']]['name'].tolist()
    work_locations = df.loc[df['field'] == 'Work Location', ['name']]['name'].tolist()

# Upload the value and table as a run output JSONValue
# named "email_outputs":
json_value_dict = {
    'cwa_locals': cwa_locals,
    'work_locations': work_locations
}
post_json_run_output(json_value_dict)

print(json_value_dict)
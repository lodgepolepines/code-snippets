from reportlab.lib.enums import TA_JUSTIFY
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
import civis
from io import BytesIO
import json
import os
from surveygizmo import SurveyGizmo
import pandas as pd
import re
import html
import numpy as np
import time
import sys

api_token = os.environ['API_TOKEN']
api_token_secret = os.environ['API_TOKEN_SECRET']

def to_markdown_table(data):
    table = '**Total New Cards: ' + str(len(data)) + '**  \n  \n'
    for i in data:
        for row in data[i]:
            if 'ACCEPTED' in row or 'DECLINED' in row:
                table += row + ': [Download Link](' + data[i][row] + ')  \n'
            else:
                table += row + ': ' + data[i][row] + '  \n'
        table += '  \n'
    return table

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

def create_civis_df(sql):
    try:
        df = civis.io.read_civis_sql(sql, database = 'CWA', use_pandas = True)
        time.sleep(5)
        print('Df created.')
    except civis.base.EmptyResultError:
        print('EmptyResultError.')
        df = pd.DataFrame()
        pass

    return df

TAG_RE = re.compile(r'<[^>]+>')
def remove_tags(text):
    return TAG_RE.sub('', text)

district_dict = {'CT':'D1','MA':'D1','ME':'D1','NH':'D1','NJ':'D1','NY':'D1','RI':'D1','VT':'D1',
'DC':'D2-13','DE':'D2-13','MD':'D2-13','PA':'D2-13','VA':'D2-13','WV':'D2-13',
'AL':'D3','FL':'D3','GA':'D3','KY':'D3','LA':'D3','MS':'D3','NC':'D3','PR':'D3','SC':'D3','TN':'D3','VI':'D3',
'IL':'D4','IN':'D4','MI':'D4','OH':'D4','WI':'D4',
'AR':'D6','KS':'D6','MO':'D6','OK':'D6','TX':'D6',
'AK':'D7','AZ':'D7','CO':'D7','IA':'D7','ID':'D7','MN':'D7','MT':'D7','ND':'D7','NE':'D7','NM':'D7','OR':'D7','SD':'D7','UT':'D7','WA':'D7', 'WY':'D7',
'CA':'D9','HI':'D9','NV':'D9'}

sgclient = SurveyGizmo(
    api_version='v5',
    api_token = api_token,
    api_token_secret = api_token_secret
)

filtered = sgclient.api.surveyresponse.filter('status', '=', 'Complete')
survey_ids = ['5538357', '6086091', '6246995', '6086084', '6086010', '6246996', '6086031', '6086029', '6205420', '6086096']

session_df = pd.DataFrame(columns = ['session_id', 'id', 'survey_id', 'contract_name'])

for responses in survey_ids:

    # get contract name
    retries = 0
    success = False
    while not success and retries < 10:
        try:
            contract = sgclient.api.survey.get(responses)
            success = True
        except Exception as e:
            wait = retries * 5
            print('Error! Waiting ' + str(wait) + ' seconds and retrying...')
            time.sleep(wait)
            retries += 1
            if retries == 10:
                sys.exit("Unable to connect.")

    contract_name = contract['data']['title']

    # get total pages
    retries = 0
    success = False
    while not success and retries < 10:
        try:
            total_pages = filtered.list(responses, resultsperpage=500)['total_pages']
            success = True
        except Exception as e:
            wait = retries * 5
            print('Error! Waiting ' + str(wait) + ' seconds and retrying...')
            time.sleep(wait)
            retries += 1
            if retries == 10:
                sys.exit("Unable to connect.")

    # get all responses
    for page in range(1, total_pages + 1):

        retries = 0
        success = False
        while not success and retries < 10:
            try:
                response = filtered.list(responses, resultsperpage=500, page=page)
                success = True
            except Exception as e:
                wait = retries * 5
                print('Error! Waiting ' + str(wait) + ' seconds and retrying...')
                time.sleep(wait)
                retries += 1

        for i in enumerate(response['data']):
            session_df2 = pd.DataFrame([[i[1]['session_id'], i[1]['id'], responses, contract_name]], columns = ['session_id', 'id', 'survey_id', 'contract_name'])
            session_df = session_df.append(session_df2, ignore_index=True)

sql = '''select * from sandbox_organizing.att_ecards'''
vf = create_civis_df(sql)

old_session_ids = vf['session_id'].tolist()
new_session_ids = session_df['session_id'].tolist()
old_vol_ids = [x for x in old_session_ids if str(x) != 'nan']
new_vol_ids = [x for x in new_session_ids if str(x) != 'nan']
c = set(old_vol_ids)
d = set(new_vol_ids)

data_dict = {
            'D1': {},
            'D2-13': {},
            'D3': {},
            'D4': {},
            'D6': {},
            'D7': {},
            'D9': {}
        }

if c==d:
    print('The session_id lists are equal. No need to create new e-card PDFs.')
else:
    print('The session_id lists are not equal. Creating new e-card PDFs.')
    diff1 = list(d-c)
    print(diff1)

    if not diff1:
        print('Empty list.')
    else:

        member_df = pd.DataFrame(columns = ['session_id','Contract', 'First Name', 'Last Name', 'Work Location Address', 'Work Location State',
        'Employee ID', 'Local No.', 'Home Address', 'City', 'State', 'Zip', 'Personal Email Address', 'Personal Cell Phone', 'Opt-In',
        'Timestamp', 'IP', 'Secondary Survey ID', 'Membership', 'Electronic Signature'])

        dues_df = pd.DataFrame(columns = ['Dues session_id', 'Contract', 'Intro Statement','First Name', 'Last Name', 'Work Location Address', 'Work Location State', 'Employee ID',
        'Local No.', 'Home Address', 'City', 'State', 'Zip', 'Personal Email Address', 'Personal Cell Phone',
        'Timestamp', 'IP', 'Dues Deduction Authorization', 'Electronic Signature'])

        cope_df = pd.DataFrame(columns = ['COPE session_id', 'Contract', 'First Name', 'Last Name', 'Occupation', 'Employee ID', 'Local No.', 'Home Address',
        'City', 'State', 'Zip', 'Personal Email Address', 'Personal Cell Phone', 'Amount to Deduct Per Pay Period',
        'Select One', 'IP', 'Timestamp', 'Political Contributions Authorization', 'Electronic Signature'])

        for val in diff1:
            id_val = session_df.loc[session_df['session_id'] == val, 'id'].iloc[0]
            survey_id_val = session_df.loc[session_df['session_id'] == val, 'survey_id'].iloc[0]

            retries = 0
            success = False

            while not success and retries < 10:
                try:
                    i = sgclient.api.surveyresponse.get(survey_id = survey_id_val, response_id = id_val)
                    success = True
                except Exception as e:
                    wait = retries * 5
                    print('Error! Waiting ' + str(wait) + ' seconds and retrying...')
                    time.sleep(wait)
                    retries += 1
            
            contract_name_val = session_df.loc[session_df['session_id'] == val, 'contract_name'].iloc[0]

            if contract_name_val == 'AT&T Southeast Utility E-card':
                local_dropdown = i['data']['survey_data']['4']['subquestions']['168']['answer']
            else:
                try:
                    print(contract_name_val)
                    local_dropdown = i['data']['survey_data']['4']['subquestions']['170']['answer']
                except:
                    local_dropdown = i['data']['survey_data']['4']['subquestions']['80']['answer']

            try:
                membership_optin = i['data']['survey_data']['4']['subquestions']['33']['options']['10024']['option']
            except:
                membership_optin = 'No'

            print(membership_optin)

            member_df2 = pd.DataFrame([[
            i['data']['session_id'], #sessionID
            contract_name_val, #contract
            i['data']['survey_data']['4']['subquestions']['5']['answer'], #fname
            i['data']['survey_data']['4']['subquestions']['6']['answer'], #lname
            i['data']['survey_data']['4']['subquestions']['72']['answer'], #work location
            i['data']['survey_data']['4']['subquestions']['156']['answer'], #work state
            i['data']['survey_data']['4']['subquestions']['148']['answer'], #emp id
            local_dropdown, #local
            i['data']['survey_data']['4']['subquestions']['9']['answer'], #home addr
            i['data']['survey_data']['4']['subquestions']['11']['answer'], #city
            i['data']['survey_data']['4']['subquestions']['160']['answer'], #state
            i['data']['survey_data']['4']['subquestions']['13']['answer'], #zip
            i['data']['survey_data']['4']['subquestions']['15']['answer'], #email
            i['data']['survey_data']['4']['subquestions']['18']['answer'], #cell
            membership_optin,
            i['data']['survey_data']['59']['answer'], #timestamp
            i['data']['survey_data']['58']['answer'], #ip
            i['data']['session_id'], #surveyid
            remove_tags(i['data']['survey_data']['101']['answer']), #membership
            i['data']['survey_data']['68']['answer'] #signature
            ]], columns = ['session_id','Contract', 'First Name', 'Last Name', 'Work Location Address', 'Work Location State',
            'Employee ID', 'Local No.', 'Home Address', 'City', 'State', 'Zip', 'Personal Email Address', 'Personal Cell Phone', 'Opt-In',
            'Timestamp', 'IP', 'Secondary Survey ID', 'Membership', 'Electronic Signature'])
            member_df = member_df.append(member_df2, ignore_index=True)

            dues_df2 = pd.DataFrame([[
            i['data']['session_id'], #sessionID
            contract_name_val,
            remove_tags(i['data']['survey_data']['81']['question']),
            i['data']['survey_data']['81']['subquestions']['82']['answer'],
            i['data']['survey_data']['81']['subquestions']['83']['answer'],
            i['data']['survey_data']['81']['subquestions']['84']['answer'],
            i['data']['survey_data']['81']['subquestions']['158']['answer'],
            i['data']['survey_data']['4']['subquestions']['148']['answer'], #emp id - #169 for dues?
            local_dropdown,
            i['data']['survey_data']['81']['subquestions']['87']['answer'],
            i['data']['survey_data']['81']['subquestions']['88']['answer'],
            i['data']['survey_data']['81']['subquestions']['89']['answer'],
            i['data']['survey_data']['81']['subquestions']['90']['answer'],
            i['data']['survey_data']['81']['subquestions']['91']['answer'],
            i['data']['survey_data']['81']['subquestions']['92']['answer'],
            i['data']['survey_data']['133']['answer'],
            i['data']['survey_data']['135']['answer'],
            remove_tags(i['data']['survey_data']['167']['answer']),
            remove_tags(i['data']['survey_data']['131']['answer'])]], 
            columns = ['Dues session_id', 'Contract','Intro Statement', 'First Name', 'Last Name', 'Work Location Address', 'Work Location State', 'Employee ID',
            'Local No.', 'Home Address', 'City', 'State', 'Zip', 'Personal Email Address', 'Personal Cell Phone',
            'Timestamp', 'IP', 'Dues Deduction Authorization', 'Electronic Signature'])
            dues_df = dues_df.append(dues_df2, ignore_index=True)

            if 'answer' in i['data']['survey_data']['129']:
                deduction_amount = i['data']['survey_data']['129']['answer']
            else:
                deduction_amount = 'None'

            if 'answer' in i['data']['survey_data']['130']:
                deduction_type = i['data']['survey_data']['130']['answer']
            else:
                deduction_type = 'None'

            cope_df2 = pd.DataFrame([[
            i['data']['session_id'], #sessionID
            contract_name_val,
            i['data']['survey_data']['115']['subquestions']['116']['answer'],
            i['data']['survey_data']['115']['subquestions']['117']['answer'],
            i['data']['survey_data']['115']['subquestions']['150']['answer'],
            i['data']['survey_data']['4']['subquestions']['148']['answer'], #emp id - #169 for dues?
            local_dropdown,
            i['data']['survey_data']['115']['subquestions']['121']['answer'],
            i['data']['survey_data']['115']['subquestions']['122']['answer'],
            i['data']['survey_data']['115']['subquestions']['159']['answer'],
            i['data']['survey_data']['115']['subquestions']['124']['answer'],
            i['data']['survey_data']['115']['subquestions']['125']['answer'],
            i['data']['survey_data']['115']['subquestions']['126']['answer'],
            deduction_amount,
            deduction_type,
            i['data']['survey_data']['137']['answer'],
            i['data']['survey_data']['136']['answer'],
            remove_tags(i['data']['survey_data']['162']['answer']),
            remove_tags(i['data']['survey_data']['138']['answer'])
            ]],
            columns = ['COPE session_id','Contract','First Name', 'Last Name', 'Occupation', 'Employee ID', 'Local No.', 'Home Address',
            'City', 'State', 'Zip', 'Personal Email Address', 'Personal Cell Phone', 'Amount to Deduct Per Pay Period',
            'Select One', 'IP', 'Timestamp', 'Political Contributions Authorization', 'Electronic Signature'])
            cope_df = cope_df.append(cope_df2, ignore_index=True)

        member_df['Timestamp 2'] = pd.to_datetime(member_df['Timestamp'])
        member_df['Timestamp 2'] = member_df['Timestamp 2'].dt.strftime('%Y-%m-%d %Hhr%Mm%Ss')
        dues_df['Timestamp 2'] = pd.to_datetime(dues_df['Timestamp'])
        dues_df['Timestamp 2'] = dues_df['Timestamp 2'].dt.strftime('%Y-%m-%d %Hhr%Mm%Ss')
        cope_df['Timestamp 2'] = pd.to_datetime(cope_df['Timestamp'])
        cope_df['Timestamp 2'] = cope_df['Timestamp 2'].dt.strftime('%Y-%m-%d %Hhr%Mm%Ss')

        member_df = member_df.sort_values(by='session_id', ascending=False)

        newdues_df = dues_df.copy()
        newdues_df.columns = ['DUES ' + str(col) for col in newdues_df.columns]

        newcope_df = cope_df.copy()
        newcope_df.columns = ['COPE ' + str(col) for col in newcope_df.columns]

        duescope_df = pd.merge(newdues_df, newcope_df, left_on='DUES Dues session_id', right_on='COPE COPE session_id', how='left')
        total_df = pd.merge(member_df, duescope_df, left_on='session_id', right_on='DUES Dues session_id', how='left')

        total_df['District'] = total_df['Work Location State'].map(district_dict)

        total_df = total_df.drop(columns=['DUES Employee ID', 'COPE Employee ID'])

        total_df['Member URL'] = np.nan
        total_df['Dues URL'] = np.nan
        total_df['COPE URL'] = np.nan

        # initialize Civis client
        client = civis.APIClient()

        total_df.sort_values(by=['Local No.'])
        sorted_ids = total_df['session_id']

        for session_id in sorted_ids:
            new_member_df = member_df.loc[member_df['session_id'] == session_id]
            print(new_member_df)
            for index, row in new_member_df.iterrows():
                buffer = BytesIO()
                member_title = '<b>Communications Workers of America Membership Form</b>'
                contract = '<b>Contract: </b><br />' + html.escape(row['Contract'])
                contract_title = row['Contract']
                first_name = '<b>First Name: </b><br />' + row['First Name']
                first_name_title = row['First Name']
                last_name = '<b>Last Name: </b><br />' + row['Last Name']
                last_name_title = row['Last Name']
                work_location_addr = '<b>Work Location Address: </b><br />' +  row['Work Location Address']
                work_location_state = '<b>Work Location State: </b><br />' +  row['Work Location State']
                card_district = district_dict[row['Work Location State']]
                empid = '<b>Employee ID: </b><br />' + row['Employee ID']
                local = '<b>Local No: </b><br />' +  row['Local No.']
                home_addr = '<b>Home Address: </b><br />' +  row['Home Address']
                city = '<b>City: </b><br />' +  row['City']
                state = '<b>State: </b><br />' + row['State']
                state_title = row['State']
                home_zip = '<b>Zip: </b><br />' + row['Zip']
                personal_email = '<b>Personal Email Address: </b><br />' + row['Personal Email Address']
                personal_cell = '<b>Personal Cell Phone: </b><br />' + row['Personal Cell Phone']
                optin = '<b>Text Opt-In: </b><br />' + row['Opt-In']
                timestamp = '<b>Timestamp: </b><br />' + row['Timestamp']
                timestamp2 = row['Timestamp 2']
                ip_address = '<b>IP Address: </b><br />' + row['IP']
                surveyid = '<b>Secondary Survey ID: </b><br />' + row['Secondary Survey ID']
                member = '<b>Membership: </b><br />' + html.escape(row['Membership'])
                signature = '<b>Electronic Signature: </b><br />' + row['Electronic Signature']

                if row['Membership'] == '''No, I decline membership. I understand I don't get to vote for local union officers or on contracts.''':
                    membership_status = 'MEMBERSHIP DECLINED'
                else:
                    membership_status = 'MEMBERSHIP ACCEPTED'

                pdf_name = last_name_title + ' ' + first_name_title + '-MEMBER-' + membership_status + '-' + contract_title + '-' + state_title + '-' + timestamp2 + '.pdf'
                doc = SimpleDocTemplate(buffer,pagesize=letter,
                                    rightMargin=48,leftMargin=48,
                                    topMargin=24,bottomMargin=18)

                Story=[]
                pdf_parts = [contract,first_name, last_name, work_location_addr, work_location_state, empid,
                local,home_addr,city,state,home_zip,personal_email,personal_cell, optin, timestamp,ip_address,surveyid, member,signature]
                title_parts = [member_title]

                styles=getSampleStyleSheet()
                styles.add(ParagraphStyle(name='Justify', alignment=TA_JUSTIFY))
                for part in title_parts:
                    ptext = '<font size="16"><font color="red">%s</font></font>' % part.strip()
                    Story.append(Paragraph(ptext, styles["Normal"]))
                    Story.append(Spacer(1, 12))  
                for part in pdf_parts:
                    ptext = '<font size="11">%s</font>' % part.strip()
                    Story.append(Paragraph(ptext, styles["Normal"]))
                    Story.append(Spacer(1, 6))   

                doc.build(Story)

                # upload file to civis
                buffer.seek(0)
                file_id = civis.io.file_to_civis(buffer, pdf_name)
                uploaded_file = client.files.get(file_id)
                url = uploaded_file['download_url']

                data_dict[card_district].update({session_id: {'Name': first_name_title + ' ' + last_name_title, 
                                          'Contract': contract_title,
                                          'Employee ID': row['Employee ID'],
                                          'Local': row['Local No.'],
                                          'Work Location': row['Work Location Address'],
                                          'Work State': row['Work Location State'],
                                          membership_status: url}})
                
                total_df.loc[total_df['session_id'] == session_id, 'Member URL'] = url


            new_dues_df = dues_df.loc[dues_df['Dues session_id'] == session_id]
            for index, row in new_dues_df.iterrows():
                buffer = BytesIO()
                dues_title = '<b>Dues (or Equivalent Fee) Deduction Form</b>'
                contract = '<b>Contract: </b><br />' + html.escape(row['Contract'])
                contract_title = row['Contract']
                intro = html.escape(row['Intro Statement'])
                first_name = '<b>First Name: </b><br />' + row['First Name']
                first_name_title = row['First Name']
                last_name = '<b>Last Name: </b><br />' + row['Last Name']
                last_name_title = row['Last Name']
                work_location_addr = '<b>Work Location Address: </b><br />' +  row['Work Location Address']
                work_location_state = '<b>Work Location State: </b><br />' +  row['Work Location State']
                card_district = district_dict[row['Work Location State']]
                empid = '<b>Employee ID: </b><br />' + row['Employee ID']
                local = '<b>Local No: </b><br />' +  row['Local No.']
                home_addr = '<b>Home Address: </b><br />' +  row['Home Address']
                city = '<b>City: </b><br />' +  row['City']
                state = '<b>State: </b><br />' + row['State']
                state_title = row['State']
                home_zip = '<b>Zip: </b><br />' + row['Zip']
                personal_email = '<b>Personal Email Address: </b><br />' + row['Personal Email Address']
                personal_cell = '<b>Personal Cell Phone: </b><br />' + row['Personal Cell Phone']
                timestamp = '<b>Timestamp: </b><br />' + row['Timestamp']
                timestamp2 = row['Timestamp 2']
                ip_address = '<b>IP Address: </b><br />' + row['IP']
                dues_deduction = '<b>Dues Deduction Authorization: </b><br />' + html.escape(row['Dues Deduction Authorization'])
                signature = '<b>Electronic Signature: </b><br />' + row['Electronic Signature']

                if row['Dues Deduction Authorization'] == '''No, I choose to opt out of payroll dues deduction.''':
                    dues_status = 'DUES DECLINED'
                else:
                    dues_status = 'DUES ACCEPTED'

                pdf_name = last_name_title + ' ' + first_name_title + '-DUES-' + dues_status + '-' + contract_title + '-' + state_title + '-' + timestamp2 + '.pdf'
                doc = SimpleDocTemplate(buffer,pagesize=letter,
                                    rightMargin=48,leftMargin=48,
                                    topMargin=24,bottomMargin=18)

                Story=[]
                pdf_parts = [first_name, last_name, work_location_addr, work_location_state, empid,
                local,home_addr,city,state,home_zip,personal_email,personal_cell,timestamp,ip_address,dues_deduction,signature]
                title_parts = [dues_title]
                contract_parts = [contract]
                intro_parts = [intro]

                styles=getSampleStyleSheet()
                styles.add(ParagraphStyle(name='Justify', alignment=TA_JUSTIFY))
                for part in title_parts:
                    ptext = '<font size="14"><font color="red">%s</font></font>' % part.strip()
                    Story.append(Paragraph(ptext, styles["Normal"]))
                    Story.append(Spacer(1, 12))

                for part in contract_parts:
                    ptext = '<font size="11">%s</font>' % part.strip()
                    Story.append(Paragraph(ptext, styles["Normal"]))
                    Story.append(Spacer(1, 12))

                for part in intro_parts:
                    ptext = '<font size="8">%s</font>' % part.strip()
                    Story.append(Paragraph(ptext, styles["Normal"]))
                    Story.append(Spacer(1, 12))
                
                for part in pdf_parts:
                    ptext = '<font size="10">%s</font>' % part.strip()
                    Story.append(Paragraph(ptext, styles["Normal"]))
                    Story.append(Spacer(1, 6))

                doc.build(Story)

                # upload file to civis
                buffer.seek(0)
                file_id = civis.io.file_to_civis(buffer, pdf_name)
                uploaded_file = client.files.get(file_id)
                url = uploaded_file['download_url']

                data_dict[card_district][session_id].update({
                                          dues_status: url})

                total_df.loc[total_df['session_id'] == session_id, 'Dues URL'] = url


            new_cope_df = cope_df.loc[cope_df['COPE session_id'] == session_id]
            for index, row in new_cope_df.iterrows():
                buffer = BytesIO()
                cope_title =  '<b>Political Contributions Committee Payroll Deduction Form</b>'
                cope_contract = '<b>Contract: </b><br />' + html.escape(row['Contract'])
                cope_contract_title = row['Contract']
                cope_first_name = '<b>First Name: </b><br />' + row['First Name']
                cope_first_name_title = row['First Name']
                cope_last_name = '<b>Last Name: </b><br />' + row['Last Name']
                cope_last_name_title = row['Last Name']
                cope_occupation = '<b>Occupation: </b><br />' + row['Occupation']
                cope_empid = '<b>Employee ID: </b><br />' + row['Employee ID']
                cope_local = '<b>Local No: </b><br />' +  row['Local No.']
                cope_home_addr = '<b>Home Address: </b><br />' +  row['Home Address']
                cope_city = '<b>City: </b><br />' +  row['City']
                cope_state = '<b>State: </b><br />' + row['State']
                cope_state_title = row['State']
                card_district = district_dict[row['State']]
                cope_home_zip = '<b>Zip: </b><br />' + row['Zip']
                cope_personal_email = '<b>Personal Email Address: </b><br />' + row['Personal Email Address']
                cope_personal_cell = '<b>Personal Cell Phone: </b><br />' + row['Personal Cell Phone']
                cope_timestamp = '<b>Timestamp: </b><br />' + row['Timestamp']
                cope_timestamp2 = row['Timestamp 2']
                cope_ip_address = '<b>IP Address: </b><br />' + row['IP']
                cope_amount = '<b>Amount to Deduct Per Pay Period </b><br />' + row['Amount to Deduct Per Pay Period']
                cope_select_one = '<b>Select One: </b><br />' + row['Select One']
                cope_authorization = '<b>Political Contributions Authorization: </b><br />' + row['Political Contributions Authorization']
                cope_signature = '<b>Electronic Signature: </b><br />' + row['Electronic Signature']

                if row['Political Contributions Authorization'] == '''No, I choose to opt out.''':
                    cope_status = 'COPE DECLINED'
                else:
                    cope_status = 'COPE ACCEPTED'            

                pdf_name = cope_last_name_title + ' ' + cope_first_name_title + '-COPE-' + cope_status + '-' + row['Select One'] + '-' + row['Amount to Deduct Per Pay Period'] + '-' + cope_contract_title + '-' + cope_state_title + '-' + cope_timestamp2 + '.pdf'
                doc = SimpleDocTemplate(buffer,pagesize=letter,
                                    rightMargin=48,leftMargin=48,
                                    topMargin=24,bottomMargin=18)

                Story=[]
                cope_pdf_parts = [cope_contract, cope_first_name, cope_last_name, cope_occupation, cope_empid, cope_local, cope_home_addr, cope_city, cope_state, 
                cope_home_zip, cope_personal_email, cope_personal_cell, cope_amount, cope_select_one, cope_ip_address, cope_timestamp, cope_authorization, cope_signature]
                cope_title_parts = [cope_title]

                styles=getSampleStyleSheet()
                styles.add(ParagraphStyle(name='Justify', alignment=TA_JUSTIFY))

                for part in cope_title_parts:
                    ptext = '<font size="16"><font color="red">%s</font></font>' % part.strip()
                    Story.append(Paragraph(ptext, styles["Normal"]))
                    Story.append(Spacer(1, 12))  
                for part in cope_pdf_parts:
                    ptext = '<font size="12">%s</font>' % part.strip()
                    Story.append(Paragraph(ptext, styles["Normal"]))
                    Story.append(Spacer(1, 6))   

                doc.build(Story)

                # upload file to civis
                buffer.seek(0)
                file_id = civis.io.file_to_civis(buffer, pdf_name)
                uploaded_file = client.files.get(file_id)
                url = uploaded_file['download_url']

                data_dict[card_district][session_id].update({cope_status: url})

                total_df.loc[total_df['session_id'] == session_id, 'COPE URL'] = url

for i in data_dict:
    if data_dict[i]:
        tbl = to_markdown_table(data_dict[i])
        data_dict[i].update({'table': tbl})
        data_dict[i].update({'status': True})
    else:
        data_dict[i].update({'status': False})

post_json_run_output(data_dict)

#        full_df = pd.concat([total_df, vf])
#        df_append = civis.io.dataframe_to_civis(full_df,
#                    database='CWA',
#                    table='sandbox_organizing.att_ecards',
#                    existing_table_rows='append')
#        print(df_append.result())
#
#        print('ATT sign-up list updated!')

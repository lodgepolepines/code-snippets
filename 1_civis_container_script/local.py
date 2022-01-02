from dotenv import load_dotenv
from upload import ABUploader
from surveygizmo import SurveyGizmo
import pandas as pd
import re
import os
import json
from gspread_dataframe import set_with_dataframe, get_as_dataframe
import gspread
import dropbox

load_dotenv()

api_token = os.environ['API_TOKEN']
api_token_secret = os.environ['API_TOKEN_SECRET']
dbx_token = os.environ['DROPBOX_TOKEN']
spreadsheet_id = os.environ['SPREADSHEET_ID']
json_str = os.environ['JSON_STR']

def upper_to_proper_case(s):
    """
    Takes an all-caps string s and lowercases all non-leading characters
    """
    return re.sub('(\S)([A-Z]+)', lambda x : x.group(1)+x.group(2).lower(), s)

def to_dropbox(dataframe, path, token):

    dbx = dropbox.Dropbox(token)

    df_string = dataframe.to_csv(index=False)
    db_bytes = bytes(df_string, 'utf8')
    dbx.files_upload(
        f=db_bytes,
        path=path,
        mode=dropbox.files.WriteMode.overwrite
    )

client = SurveyGizmo(
    api_version='v5',
    api_token = api_token,
    api_token_secret = api_token_secret
)

filtered = client.api.surveyresponse.filter('status', '=', 'Complete')
survey_id = os.environ['SURVEY_ID']
session_df = pd.DataFrame(columns = ['session_id'])
df = pd.DataFrame(columns = ['date_submitted', 'session_id', 'first_name', 'last_name', 'home_addr', 'home_city',
'home_state', 'home_zip', 'email', 'phone', 'mass_texts', 'engagement', 'studio', 'team_or_dept', 'job_title',
'work_city', 'work_state', 'facebook', 'social_media_tag_fb', 'twitter', 'social_media_tag_twitter'])
response = filtered.list(survey_id, resultsperpage=500)
total_pages = response['total_pages']

for page in range(1, total_pages + 1):
    response = filtered.list(survey_id, resultsperpage=500, page=page)
    for i in enumerate(response['data']):
        session_df2 = pd.DataFrame([[i[1]['session_id']]], columns = ['Session ID'])
        session_df = session_df.append(session_df2, ignore_index=True)

# generate json - if there are errors here remove newlines in .env
json_data = json.loads(json_str)
# the private_key needs to replace \n parsed as string literal with escaped newlines
json_data['private_key'] = json_data['private_key'].replace('\\n', '\n')

# use service_account to generate credentials object
gclient = gspread.service_account_from_dict(json_data)
print('GS creds authorized')

ch = gclient.open_by_key(spreadsheet_id)
print(ch.id)
spreadsheet_url = "https://docs.google.com/spreadsheets/d/%s" % ch.id
print(spreadsheet_url)
worksheet = ch.get_worksheet(0)
vf = get_as_dataframe(worksheet)

old_session_ids = vf['session_id'].tolist()
new_session_ids = session_df['Session ID'].tolist()
old_vol_ids = [x for x in old_session_ids if str(x) != 'nan']
new_vol_ids = [x for x in new_session_ids if str(x) != 'nan']
c = set(old_vol_ids)
d = set(new_vol_ids)

if c==d:
    print('The Session ID lists are equal.')
else:
    print('The Session ID lists are not equal.')
    diff1 = list(d-c)

    if not diff1:
        print('Empty list. Remove disqualified response from spreadsheet.')
    else:
        print(diff1)

        for page in range(1, total_pages + 1):
            response = filtered.list(survey_id, resultsperpage=500, page=page)
            for i in enumerate(response['data']):
                if i[1]['session_id'] in diff1:
                    print('Session ID found!')
                    try:
                        text_opt = i[1]['survey_data']['4']['subquestions']['70']['options']['10104']['original_answer']
                        text_opt = 'Opted-In'
                    except:
                        text_opt = '' #text opt-in

                    try:
                        supporter_opt = i[1]['survey_data']['4']['subquestions']['33']['options']['10038']['option']
                        supporter_opt = 'Public Supporter'
                    except:
                        supporter_opt = '' #public supporter opt-in

                    try:
                        fb = i[1]['survey_data']['66']['options']['10039']['original_answer']
                        social_media_tag_fb = 'Facebook Profile'
                    except:
                        fb = ''
                        social_media_tag_fb = ''
                    
                    try:
                        twitter = i[1]['survey_data']['66']['options']['10040']['original_answer']
                        if '@' in twitter:
                            sep = '@'
                            twitter = twitter.split(sep, 1)[1]
                            twitter = 'https://twitter.com/' + twitter
                            social_media_tag_twitter = 'Twitter'
                        elif twitter == 'NA' or 'N/A':
                            twitter = ''
                            social_media_tag_twitter = ''
                        else:
                            social_media_tag_twitter = 'Twitter'
                    except:
                        twitter = ''
                        social_media_tag_twitter = ''

                    df2 = pd.DataFrame([[
                    i[1]['date_submitted'], #date submitted
                    i[1]['session_id'], #sessionID
                    i[1]['survey_data']['4']['subquestions']['5']['answer'], #fname
                    i[1]['survey_data']['4']['subquestions']['6']['answer'], #lname
                    i[1]['survey_data']['4']['subquestions']['9']['answer'], #home addr
                    i[1]['survey_data']['4']['subquestions']['11']['answer'], #home city
                    i[1]['survey_data']['4']['subquestions']['12']['answer'], #home state
                    i[1]['survey_data']['4']['subquestions']['13']['answer'], #home zip
                    i[1]['survey_data']['4']['subquestions']['15']['answer'], #personal email
                    i[1]['survey_data']['4']['subquestions']['18']['answer'], #personal cell
                    text_opt, #text opt-in
                    supporter_opt, #public supporter opt-in
                    i[1]['survey_data']['71']['subquestions']['78']['answer'], #studio
                    i[1]['survey_data']['71']['subquestions']['79']['answer'], #team or dept
                    i[1]['survey_data']['71']['subquestions']['74']['answer'], #job title
                    i[1]['survey_data']['71']['subquestions']['75']['answer'], #work city
                    i[1]['survey_data']['71']['subquestions']['77']['answer'], #work state
                    fb, #FB
                    social_media_tag_fb,
                    twitter,
                    social_media_tag_twitter
                    ]], columns = ['date_submitted', 'session_id', 'first_name', 'last_name', 'home_addr', 'home_city',
                    'home_state', 'home_zip', 'email', 'phone', 'mass_texts', 'engagement', 'studio', 'team_or_dept', 'job_title',
                    'work_city', 'work_state', 'facebook', 'social_media_tag_fb', 'twitter', 'social_media_tag_twitter'])
                    df = df.append(df2, ignore_index=True)
                
                else:
                    print('Session ID not found!')

        df_obj = df.select_dtypes(['object'])
        df[df_obj.columns] = df_obj.apply(lambda x: x.str.strip())
        df['work_location'] = df['work_city'] + ', ' + df['work_state']

        df['home_addr'] = df['home_addr'].apply(upper_to_proper_case)
        df['home_city'] = df['home_city'].apply(upper_to_proper_case)
        df['auth_card'] = 'Date Signed'
        df['date_signed'] = pd.to_datetime(df['date_submitted']).dt.round('D')
        df['date_signed'] = pd.to_datetime(df['date_signed'], format = '%Y-%m-%d')
        df['job_title_note'] = df['job_title']
        df['email_subscribed'] = 'subscribed'
        df.loc[df['mass_texts'] == 'Opted-In', 'phone_subscribed'] = 'subscribed'
        df.loc[df['mass_texts'] == '', 'phone_subscribed'] = ''

        full_df = pd.concat([df, vf])
        set_with_dataframe(worksheet, full_df)
        print('Dataframe set!')
    
        #parse out two separate dfs (one for entity upload, one for info upload)
        df_entity = df[['session_id', 'first_name', 'last_name', 'home_addr', 'home_city', 'home_state', 'home_zip', 'email', 'email_subscribed',
                        'phone', 'phone_subscribed']].copy()
        df_info = df[['session_id', 'mass_texts', 'engagement', 'studio', 'team_or_dept', 'job_title', 'job_title_note', 'work_location', 'facebook', 
                    'social_media_tag_fb', 'auth_card', 'date_signed']].copy()
        df_twitter = df[['session_id', 'twitter', 'social_media_tag_twitter']].copy()

        from datetime import datetime

        # datetime object containing current date and time
        now = datetime.now()
        # dd/mm/YY H:M:S
        dt_string = now.strftime("%Y-%m-%d_%H-%M-%S")
        print(dt_string)

        df_entity.to_csv('/app/emp_entities_' + dt_string + '.csv', index = False)
        df_info.to_csv('/app/emp_info_' + dt_string + '.csv', index = False)
        df_twitter.to_csv('/app/emp_twitter_' + dt_string + '.csv', index = False)

        to_dropbox(df_entity, '/ABX CSVs/emp_entities_' + dt_string + '.csv', dbx_token)
        to_dropbox(df_info, '/ABX CSVs/emp_info_' + dt_string + '.csv', dbx_token)
        to_dropbox(df_twitter, '/ABX CSVs/emp_twitter_' + dt_string + '.csv', dbx_token)

        upload_file = '/app/emp_entities_' + dt_string + '.csv' # path to test CSV
        config_file = 'config.example.yml'
        campaign_key = 'upload-test'
        config = ABUploader.parse_config(config_file, campaign_key)
        uploader = ABUploader(config, upload_file)
        uploader.start_upload_new_entity('people')
        uploader.finish_upload()
        uploader.quit()

        upload_file = '/app/emp_info_' + dt_string + '.csv'
        config_file = 'config.info.yml'
        campaign_key = 'upload-test'
        config = ABUploader.parse_config(config_file, campaign_key)
        uploader = ABUploader(config, upload_file)
        uploader.start_upload('info')
        uploader.finish_upload()
        uploader.quit()

        upload_file = '/app/emp_twitter_' + dt_string + '.csv'
        config_file = 'config.info.yml'
        campaign_key = 'twitter-upload'
        config = ABUploader.parse_config(config_file, campaign_key)
        uploader = ABUploader(config, upload_file)
        uploader.start_upload('info')
        uploader.finish_upload()
        uploader.quit()

        print('\U0001F9D9 \U0001F525 Total records successfully uploaded \U0001F9D9 \U0001F525:')
        print(len(df.index))
        print('Example names that were uploaded:')
        print(df[['first_name', 'last_name']].head())

import pandas as pd
import time
import civis
import pysftp
import os

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


def process_barg_file(dues_file):
    dues_filepath = sftp.open(dues_file)
    print('Processing ' + dues_file)
    df = pd.read_excel(dues_filepath)

    df.rename(columns = {'Personnel number': 'fullname', 'PS text': 'pstext', 'CoCd':'company', 'Perner':'empid', 'PSubarea':'psubarea', 
    'Home Street Address':'homestreet', 'City':'homecity', 'Description':'homestate', 'Zip':'homezip', 'Amount':'amount', 'Crcy':'currency', 
    'SERVICE DATE':'servicedate', 'Work Address':'workstreet', 'Wk City':'workcity', 'Description.1':'workstate', 'Name of EG':'worktourtype'}, inplace = True)

    df['fullname'] = df['fullname'].str.upper()
    df[['firstname', 'lastname']] = df['fullname'].str.rsplit(' ', n=1, expand=True)

    # clean up locals, remove non-numeric characters
    df[['unionname', 'unionlocal']] = df['pstext'].str.split(' ', n=1, expand=True)
    df['unionlocal'] = df['unionlocal'].str.split(' ').str[0]
    df['unionlocal'] = df['unionlocal'].str.extract('(\d+)', expand=False)

    df['file'] = filename
    df['homestate'] = df['homestate'].str.upper()
    df['workstate'] = df['workstate'].str.upper()
    df['homestreet'] = df['homestreet'].str.upper()
    df['homecity'] = df['homecity'].str.upper()
    df['currency'] = 'USDN'
    df['servicedate'] = pd.to_datetime(df['servicedate'])

    state_dict = {'ALASKA': 'AK', 'ALABAMA': 'AL', 'ARKANSAS': 'AR', 'ARIZONA': 'AZ', 'CALIFORNIA': 'CA', 'COLORADO': 'CO', 'CONNECTICUT': 'CT', 
    'DISTRICT OF COLUMBIA': 'DC', 'DELAWARE': 'DE', 'FLORIDA': 'FL', 'GEORGIA': 'GA', 'HAWAII': 'HI', 'IOWA': 'IA', 'IDAHO': 'ID', 'ILLINOIS': 'IL', 
    'INDIANA': 'IN', 'KANSAS': 'KS', 'KENTUCKY': 'KY', 'LOUISIANA': 'LA', 'MASSACHUSETTS': 'MA', 'MARYLAND': 'MD', 'MAINE': 'ME', 'MICHIGAN': 'MI', 
    'MINNESOTA': 'MN', 'MISSOURI': 'MO', 'MISSISSIPPI': 'MS', 'MONTANA': 'MT', 'NORTH CAROLINA': 'NC', 'NORTH DAKOTA': 'ND', 'NEBRASKA': 'NE', 
    'NEW HAMPSHIRE': 'NH', 'NEW JERSEY': 'NJ', 'NEW MEXICO': 'NM', 'NEVADA': 'NV', 'NEW YORK': 'NY', 'OHIO': 'OH', 'OKLAHOMA': 'OK', 'OREGON': 'OR', 
    'PENNSYLVANIA': 'PA', 'PUERTO RICO': 'PR', 'RHODE ISLAND': 'RI', 'SOUTH CAROLINA': 'SC', 'SOUTH DAKOTA': 'SD', 'TENNESSEE': 'TN', 'TEXAS': 'TX', 
    'UTAH': 'UT', 'VIRGINIA': 'VA', 'VERMONT': 'VT', 'WASHINGTON': 'WA', 'WISCONSIN': 'WI', 'WEST VIRGINIA': 'WV', 'WYOMING': 'WY', 'US VIRGIN ISLANDS': 'VI'}

    df['workstate_acronym'] = df['workstate'].map(state_dict)

    df = df[['file', 'company', 'empid', 'fullname', 'lastname', 'firstname', 'psubarea', 'pstext', 'unionname', 'unionlocal', 
    'homestreet', 'homecity', 'homestate', 'homezip', 'amount', 'currency', 'servicedate', 'workstreet', 'workcity', 'workstate', 
    'worktourtype', 'workstate_acronym']]

    print(dues_file + ' processed.')

    return df


def load_barg_file(df):
    print("UPDATING CIVIS LUMEN BARG TABLE...")

    df_append = civis.io.dataframe_to_civis(df,
                                database='CWA',
                                table='org_lumen.barg',
                                existing_table_rows='append')
    print(df_append.result())


def process_dues_file(dues_file):
    dues_filepath = sftp.open(dues_file)
    print('Processing ' + dues_file)
    df = pd.read_excel(dues_filepath)

    df.rename(columns = {'Full Employee Name': 'fullname', 'Company Code':'company', 'Personnel No':'empid', 'emp_uuid':'uuid', 'WAGE TYPE':'wagetype',
    'WAGE TYPE DESCRIPTIO':'wagetypedesc', 'Amount':'amount', 'Personnel Subarea':'perssubarea', 'PS Text':'local'}, inplace = True)

    df['fullname'] = df['fullname'].str.upper()
    df[['lastname', 'firstname']] = df['fullname'].str.rsplit(' ', n=1, expand=True)
    df['file'] = filename
    df['local'] = df['local'].str.extract('(\d+)', expand=False)
    df['local'] = df['local'].str.lstrip('0')

    df = df[['file', 'company', 'empid', 'uuid', 'fullname', 'lastname', 'firstname', 'wagetype', 'wagetypedesc', 'amount', 'perssubarea', 'local']]

    print(dues_file + ' processed.')

    return df


def load_dues_file(df):
    print('UPDATING CIVIS LUMEN DUES TABLE...')

    df_append = civis.io.dataframe_to_civis(df,
                        database='CWA',
                        table='org_lumen.dues',
                        existing_table_rows='append')
    print(df_append.result())


host = os.environ['HOST']
user = os.environ['USER']
pw = os.environ['PW']


start_loc = "groups/ORGANIZE/!Data/Lumen/Unprocessed Dues Files/"
end_loc = "groups/ORGANIZE/!Data/Lumen/Processed Dues Files/"

with pysftp.Connection(host,
                       username=user,
                       password=pw,
                       port=15013
                       ) as sftp:
    start_list = sftp.listdir(start_loc)
    end_list = sftp.listdir(end_loc)
    diff_list = list(set(start_list) - set(end_list))

    for filename in diff_list:
        if 'barg' in filename:
            df = process_barg_file(dues_file=start_loc + filename)
            load_barg_file(df)
            sftp.rename(start_loc + filename, end_loc + filename)
            print('Transferred processed dues file to Processed folder.')
        else:
            df = process_dues_file(dues_file=start_loc + filename)
            load_dues_file(df)
            sftp.rename(start_loc + filename, end_loc + filename)
            print('Transferred processed dues file to Processed folder.')
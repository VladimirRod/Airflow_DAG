from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from google.oauth2 import service_account
from googleapiclient.discovery import build
from datetime import date, timedelta, datetime
from dateutil.parser import parse
import time
import json
import gspread
import datetime


default_args = {
    'owner': 'Admin',
    'start_date': datetime.datetime(2023, 3, 21),
}

dag = DAG(
    'export_users_from_gworkspace_to_gsheet',
    default_args=default_args,
    schedule_interval='0 1 * * *',
    start_date=datetime.datetime(2023, 3, 21),
)


def export_users_from_gworkspace():
    serv_acc = gspread.service_account(filename='/path/to/creds.json')
    sheet_id = serv_acc.open_by_key('GOOGLE_SHEET_ID_HERE')
    worksheet = sheet_id.get_worksheet(1)

    SERVICE_ACCOUNT_FILE = '/path/to/creds.json'
    DELEGATE = 'ADMIN_EMAIL_GWORKSPACE_HERE'
    TARGET = 'DOMAIN_HERE'
    credentials = service_account.Credentials.from_service_account_file(
        filename=SERVICE_ACCOUNT_FILE,
        scopes=['https://www.googleapis.com/auth/admin.directory.user.readonly']
    )
    credentials_delegated = credentials.with_subject(DELEGATE)

    service = build('admin', 'directory_v1', credentials=credentials_delegated, cache_discovery=False)

    offset = datetime.timezone(datetime.timedelta(hours=3))
    update = datetime.datetime.now(offset).replace(microsecond=0)
    updated_msk = json.dumps(update, default=str).replace('"', '')

    worksheet.clear()
    worksheet.append_row(['Последнее обновление:' + updated_msk])
    worksheet.append_row(['FullName', 'Email Address', 'Org Unit Path', 'Last Sign In', 'Work Secondary Email', 'Recovery Phone'])

    results = service.users().list(domain=TARGET).execute()
    for user in range(len(results['users'])):
        name = results['users'][user]['name']['fullName']
        primary_email = results['users'][user]['primaryEmail']
        org_path_unit = results['users'][user]['orgUnitPath']
        last_login = results['users'][user]['lastLoginTime']
        for j in range(len(results['users'][user]['emails'])):
            if 'type' in results['users'][user]['emails'][j]:
                alter_email = results['users'][user]['emails'][j]['address']
            else:
                alter_email = 'None'
        if 'recoveryPhone' in results['users'][user]:
            recovery_phone = results['users'][user]['recoveryPhone']
        else:
            recovery_phone = 'None'
        worksheet.append_row([name, primary_email, org_path_unit, last_login, alter_email, recovery_phone])
        time.sleep(1)

    while results.get('nextPageToken'):
        token = results.get('nextPageToken')
        results = service.users().list(domain=TARGET, pageToken=token).execute()
        for user_c in range(len(results['users'])):
            name = results['users'][user_c]['name']['fullName']
            primary_email = results['users'][user_c]['primaryEmail']
            org_path_unit = results['users'][user_c]['orgUnitPath']
            last_login = results['users'][user_c]['lastLoginTime']
            for j in range(len(results['users'][user_c]['emails'])):
                if 'type' in results['users'][user_c]['emails'][j]:
                    alter_email = results['users'][user_c]['emails'][j]['address']
                else:
                    alter_email = 'None'
            if 'recoveryPhone' in results['users'][user_c]:
                recovery_phone = results['users'][user_c]['recoveryPhone']
            else:
                recovery_phone = 'None'
            worksheet.append_row([name, primary_email, org_path_unit, last_login, alter_email, recovery_phone])
            time.sleep(1)


python_task = PythonOperator(task_id='export_users_info', python_callable=export_users_from_gworkspace)

dummy_task = DummyOperator(task_id='dummy_task', dag=dag)

dummy_task >> python_task

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
import datetime
import time
import json
import gspread
from slack_sdk import WebClient


default_args = {
    'owner': 'Admin',
    'start_date': datetime.datetime(2023, 3, 21),
}

dag = DAG(
    'export_users_from_gworkspace_to_gsheet',
    default_args=default_args,
    schedule_interval='0 4 * * *',
    start_date=datetime(2023, 3, 21),
)


def slack_user_profile():
    client = WebClient(token='SLACK_BOT_TOKEN_HERE')
    serv_acc = gspread.service_account(filename='creds.json')
    sheet_id = serv_acc.open_by_key('GOOGLE_SHEET_ID_HERE')
    worksheet = sheet_id.get_worksheet(0)

    offset = datetime.timezone(datetime.timedelta(hours=3))
    update = datetime.datetime.now(offset).replace(microsecond=0)
    updated_msk = json.dumps(update, default=str).replace('"', '')

    users = client.users_list(limit=1000)
    users_id_list = []

    for i in range(len(users["members"])):
        if users["members"][i]['deleted'] is False and users["members"][i]['is_bot'] is False and users["members"][i]['is_restricted'] is False and users["members"][i]['is_ultra_restricted'] is False:
            users_id_list.append(users["members"][i]['id'])

    while users.get('response_metadata')['next_cursor']:
        users = client.users_list(cursor=users.get('response_metadata')['next_cursor'], limit=5000)
        for i in range(len(users["members"])):
            if users["members"][i]['deleted'] is False and users["members"][i]['is_bot'] is False and \
                    users["members"][i]['is_restricted'] is False and users["members"][i]['is_ultra_restricted'] is False:
                users_id_list.append(users["members"][i]['id'])

    worksheet.clear()
    worksheet.append_row(['Последнее обновление: ' + updated_msk])
    worksheet.append_row(['ID', 'real_name', 'display_name', 'phone', 'email', 'birthday', 'TG', 'school', 'gmail', 'first_day', 'department', 'position', 'country', 'city'])

    for i in users_id_list:
        user_info = client.users_profile_get(user=i)
        user_id = i
        if user_info['profile']['phone']:
            phone = user_info['profile']['phone']
        else:
            phone = 'None'
        if user_info['profile']['real_name']:
            real_name = user_info['profile']['real_name']
        else:
            real_name = 'None'
        if user_info['profile']['real_name_normalized']:
            real_name_normalized = user_info['profile']['real_name_normalized']
        else:
            real_name_normalized = 'None'
        if user_info['profile']['display_name']:
            display_name = user_info['profile']['display_name']
        else:
            display_name = 'None'
        if user_info['profile']['display_name_normalized']:
            display_name_normalized = user_info['profile']['display_name_normalized']
        else:
            display_name_normalized = 'None'
        if 'email' in user_info['profile']:
            email = user_info['profile']['email']
        else:
            email = 'None'
        if 'Xf03BQ898N85' in user_info['profile']['fields']:
            birthday = user_info['profile']['fields']['Xf03BQ898N85']['value']
        else:
            birthday = 'None'
        if 'Xf03BUHJLBK5' in user_info['profile']['fields']:
            telegram = user_info['profile']['fields']['Xf03BUHJLBK5']['value']
        else:
            telegram = 'None'
        if 'Xf03BUPMT96W' in user_info['profile']['fields']:
            school = user_info['profile']['fields']['Xf03BUPMT96W']['value']
        else:
            school = 'None'
        if 'Xf03BRLSA7JR' in user_info['profile']['fields']:
            gmail = user_info['profile']['fields']['Xf03BRLSA7JR']['value']
        else:
            gmail = 'None'
        if 'Xf03BUH7E58S' in user_info['profile']['fields']:
            first_day = user_info['profile']['fields']['Xf03BUH7E58S']['value']
        else:
            first_day = 'None'
        if 'Xf03BUPWA75H' in user_info['profile']['fields']:
            department = user_info['profile']['fields']['Xf03BUPWA75H']['value']
        else:
            department = 'None'
        if 'Xf03RVMF5WJF' in user_info['profile']['fields']:
            position = user_info['profile']['fields']['Xf03RVMF5WJF']['value']
        else:
            position = 'None'
        if 'Xf04KH6QTZKN' in user_info['profile']['fields']:
            city = user_info['profile']['fields']['Xf04KH6QTZKN']['value']
        else:
            city = 'None'
        if 'Xf04PJCV58AK' in user_info['profile']['fields']:
            country = user_info['profile']['fields']['Xf04PJCV58AK']['value']
        else:
            country = 'None'

        worksheet.append_row([user_id, real_name, display_name, phone, email, birthday, telegram, school, gmail, first_day, department, position, country, city])
        time.sleep(2)


python_task = PythonOperator(task_id='export_users_info_from_slack', python_callable=slack_user_profile)

dummy_task = DummyOperator(task_id='dummy_task', dag=dag)

dummy_task >> python_task

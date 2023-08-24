from datetime import datetime
from airflow import DAG
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.providers.ssh.operators.ssh import SSHOperator
from slack_notifications import SlackAlert
from airflow.models import Variable


slack_api_token = Variable.get('slack_api_token')
alert = SlackAlert('#일반', slack_api_token)


# SSHHook 객체 생성
ssh_hook = SSHHook(
    ssh_conn_id="ML",
    username="yuny",
    remote_host="34.64.249.190",
    key_file="/home/pshyeok/.ssh/id_rsa"
)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 10, 3, 0), # 시작 날짜 및 시간 설정
    'retries': 0
}

with DAG('my_dag',
         default_args=default_args,
         schedule_interval='0 3 * * *', # 스케줄 주기 설정 (매일 3시에 실행)
         catchup=False,
         on_success_callback = alert.success_msg,
         on_failure_callback = alert.fail_msg,
         ) as dag:
    
    # SSH Hook 설정
    ssh_hook = SSHHook(ssh_conn_id="ML")
    
    # SSHOperator를 이용하여 실행할 파일 경로 설정
    remote_file_path = "/home/yuny/GCP/ML/Surprise_dags/main.py"
    
    # SSHOperator를 이용하여 파일 실행
    run_file_command = f"python {remote_file_path}"
    ssh_operator = SSHOperator(task_id="run_file", ssh_hook=ssh_hook, command=run_file_command)

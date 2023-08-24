from slack_sdk import WebClient
from datetime import datetime

class SlackAlert:
	# 클래스 인스턴스 초기화
	# 채널정보와 slack 인증 토큰을 인자로 받음
    def __init__(self, channel, token):
        self.channel = channel
        self.client = WebClient(token=token)

    def success_msg(self, msg):
    	# 성공메시지를 작성하고 일자와 task id, dag id, log url을 슬랙 메세지로 출력
        text = f"""
            date : {datetime.today().strftime('%Y-%m-%d')}
            alert : 
                Success! 
                    task id : {msg.get('task_instance').task_id}, 
                    dag id : {msg.get('task_instance').dag_id}, 
                    log url : http://34.22.71.118:8080/log?execution_date={msg.get('execution_date')}&task_id={msg.get('task_instance').task_id}&dag_id={msg.get('task_instance').dag_id}&map_index=-1
            """
        self.client.chat_postMessage(channel=self.channel, text=text)

    def fail_msg(self, msg):
    	# 실패메시지를 작성하고 일자와 task id, dag id, log url을 슬랙 메세지로 출력
        text = f"""
            date : {datetime.today().strftime('%Y-%m-%d')}  
            alert : 
                Fail! 
                    task id : {msg.get('task_instance').task_id}, 
                    dag id : {msg.get('task_instance').dag_id}, 
                    log url : http://34.22.71.118:8080/log?execution_date={msg.get('execution_date')}&task_id={msg.get('task_instance').task_id}&dag_id={msg.get('task_instance').dag_id}&map_index=-1
        """
        
        

        self.client.chat_postMessage(channel=self.channel, text=text)   
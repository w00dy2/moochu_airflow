# moochu_airflow
https://velog.io/@pshyeok2/pj2-2.airflow-mongodb-%EC%A0%81%EC%9E%AC-google-storage-%EC%A0%81%EC%9E%AC
### 1. 3개의 사이트에서 4개의 정보를 가져오고 -> mongodb에 적재 -> 적재되면서 생성된 _id값으로 Google storage에 이미지 저장


1. docker-compose를 활용 airflow 실행
2. crawling 및 mongodb적재(메가박스, cgv)
  
<br>

![image](https://github.com/w00dy2/moochu_airflow/assets/123388251/43c20ba4-4bcf-41da-9392-5f84dbcaacf2)

<br>

3.google storage 적재 배치

<br>

![image](https://github.com/w00dy2/moochu_airflow/assets/123388251/99eb2a47-1863-4e3c-8957-62f0557d0016)

<br>

![image](https://github.com/w00dy2/moochu_airflow/assets/123388251/91bc348c-44ee-47f4-a023-618858150190)


### 2. 추천시스템 vm 컨테이너에 접속 -> 추천시스템 파일 실행 -> 결과 slack api로 알림
slack api

![image](https://github.com/w00dy2/moochu_airflow/assets/123388251/c9963b91-cf24-4da8-8910-b46036ecd967)

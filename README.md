# moochu_airflow
영화 추천 프로젝트 Airflow의 레포

## 관련 포스팅
<a href="https://velog.io/@pshyeok2/pj2-2.airflow-mongodb-%EC%A0%81%EC%9E%AC-google-storage-%EC%A0%81%EC%9E%AC" target="_blank"><img src="https://img.shields.io/badge/velog-20C997?style=for-the-badge&logo=velog&logoColor=white" alt="Velog"></a>


## 1. 영화 데이터 ETL 자동화 workflow

- CGV, 메가박스, 롯데시네마, 다음영화(netflix, watcha) 4개의 사이트에서 영화정보 수집
- 데이터 전처리
- 몽고db 적재
- google storage 이미지 적재
  작업의 자동화

### 1. 크롤링 및 전처리
- 각각의 사이트 별 크롤링 및 키, value의 형식을 통일시켜주는 전처리
-  <a href="https://github.com/w00dy2/moochu_airflow/blob/main/dags/Netfl_watcha_scraper.py" target="_blank">Netfl_watcha_scraper</a>
-  <a href="https://github.com/w00dy2/moochu_airflow/blob/main/dags/cgv_scraper.py" target="_blank">cgv_scraper</a>
-  <a href="https://github.com/w00dy2/moochu_airflow/tree/main/dags" target="_blank">mega_scraper.py</a>

### 2. 몽고db에 중복데이터를 확인후 적재
- 크롤링한 데이터가 앞의 작업에서 추가되어 있을경우 OTT 키에 영화관만 추가
- 기존 적재된 데이터가 없는경우 적재하면서 생성되는 _id 값을 리스트에 추가
- 각각 덱의 이름으로 _ id값을 return (google_storage에 추가하기 위함)
- return 된 리스트를 xcom으로 push
  
![image](https://github.com/w00dy2/moochu_airflow/assets/123388251/dd84d6b7-857c-4691-bd10-fbed34664afa)

### 3. 각작업에서 반환된 list를 기준으로 google storage에 포스터 이미지 적재
<a href="https://github.com/w00dy2/moochu_airflow/blob/main/dags/google_cloud_storage_upload.py" target="_blank">google_cloud_storage_upload.py</a>
- 각 작업에서 xcom push된 영화별 _id 리스트를 pull하여 하나의 리스트로 반환
- 반환된 리스트를 기준으로 mongodb에서 영화의 포스터 url을 불러와 이미지를 google storage에 적재

  
<br>

![image](https://github.com/w00dy2/moochu_airflow/assets/123388251/91bc348c-44ee-47f4-a023-618858150190)


## 2. 추천시스템 airflow workflow
<a href="https://velog.io/@pshyeok2/%EC%97%90%EC%96%B4%ED%94%8C%EB%A1%9C%EC%9A%B0%EA%B0%80-ssh-%EC%A0%91%EC%86%8D%EC%9D%B4-%EC%95%88%EB%90%9C%EB%8B%A4-%ED%82%A4-%ED%8C%8C%EC%9D%BC%EC%9D%98-%EC%9C%84%EC%B9%98%EB%A5%BC-%ED%99%95%EC%9D%B8%ED%95%98%EC%9E%90" target="_blank"><img src="https://img.shields.io/badge/velog-20C997?style=for-the-badge&logo=velog&logoColor=white" alt="Velog"></a>

매일 적재된 로그데이터를 기반으로 사용자기반 추천시스템을 업데이트 하기 위한 작업

### 1.추천시스템 GCP engine에 ssh 접속 

### 2. 추천시스템 파일 실행 

### 3. 실행결과 slack api로 알림을 통해 작업 모니터링
<a href="https://github.com/w00dy2/moochu_airflow/blob/main/dags/Netfl_watcha_scraper.py" target="_blank">slack_notifications.py</a>

![image](https://github.com/w00dy2/moochu_airflow/assets/123388251/c9963b91-cf24-4da8-8910-b46036ecd967)

## 3. Issue
### 3.1 데이터 적재 및 이미지 적재에 대한 고민
영화 포스터 이미지를 url로 바로 보여주는 방식에서 google storage에 적재하고 적재된 이미지로 보여주기로 방법을 바꾸면서 어떻게 dag을 수정할 것인가에 대한 고민이 컸다.
### 3.2 해결방안
- mongodb에서 적재하는 데이터를 조회하는 작업의 추가 없이 바로 _id값을 사용할 수 있다는 점을 알게되었다.
- xcom을 이용하면 최대 48kb의 데이터를 이용할 수 있다. (생성되는 하나의 _id는 24byte 이기 때문에 충분히 사용가능)
- 각 작업에서 생성된 _id 값을 xcom으로 push & pull하는 작업을 통해 google storage에 이미지를 적재하는 작업으로 데이터를 넘겨줄 수 있다.
  

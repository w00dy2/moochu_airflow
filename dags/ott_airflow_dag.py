import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from cgv_scraper import fetch_cgv_movies, save_cgv_movies
from mega_scraper import fetch_mega_movie, save_mega_mongo
from Netfl_watcha_scraper import update_movies_for_collections
from google_cloud_storage_upload import upload_movie_poster_to_gcs
#from google_cloud_storage_upload import google_cloud_storage_upload

default_args = {
    "owner": "pshyeok",
    "retries": 3,
    "retry_delay": datetime.timedelta(minutes=3),
    "start_date": datetime.datetime(2023, 8, 8),
}

dag = DAG(
    dag_id="movie_scraper_dag",
    default_args=default_args,
    description="Scrape movie data and upload images to Google Cloud Storage",
    schedule_interval=datetime.timedelta(days=1),
    catchup=False,
    max_active_runs=1,
)
urls = [
    'https://movie.daum.net/premovie/watcha?flag=Y',
    'https://movie.daum.net/premovie/netflix?flag=Y',
    ]

def cgv_scraper(**kwargs):
    cgv_movies = fetch_cgv_movies()
    cgv_mongo = save_cgv_movies(cgv_movies)
    kwargs['ti'].xcom_push(key='cgv_mongo', value=cgv_mongo)


def mega_scraper(**kwargs):
    merged_movies = fetch_mega_movie()
    mega_mongo = save_mega_mongo(merged_movies)
    kwargs['ti'].xcom_push(key='mega_mongo', value=mega_mongo)   

def netflix_watcha_scraper(**kwargs):
    net_mongo = update_movies_for_collections(urls)
    kwargs['ti'].xcom_push(key='net_mongo', value=net_mongo)



def merge_and_process_movies(**kwargs):
    cgv_movies = kwargs['ti'].xcom_pull(key='cgv_mongo')
    mega_movies = kwargs['ti'].xcom_pull(key='mega_mongo')
    netflix_watcha_movies = kwargs['ti'].xcom_pull(key='net_mongo')
    all_movies = cgv_movies + mega_movies + netflix_watcha_movies
    print(all_movies)
    kwargs['ti'].xcom_push(key='all_movies', value=all_movies)  # 수정된 부분

    
    
def google_cloud_storage_upload(**kwargs):
    all_movies = kwargs['ti'].xcom_pull(key='all_movies') # Merge된 데이터 가져오기
    upload_movie_poster_to_gcs(all_movies)  # all_movies 데이터를 함수 인자로 전달
    

# 파이썬 함수를 오퍼레이터로 정의
cgv_scraper_task = PythonOperator(
    task_id="cgv_scraper",
    python_callable=cgv_scraper,
    dag=dag,
)

mega_scraper_task = PythonOperator(
    task_id="mega_scraper",
    python_callable=mega_scraper,
    provide_context=True,
    dag=dag,
)

netflix_scraper_task = PythonOperator(
    task_id="netfl_watcha_scraper",
    python_callable=netflix_watcha_scraper,
    provide_context=True,
    dag=dag,
)

merge_and_process_movies = PythonOperator(
    task_id="merge_and_process_movies",
    python_callable=merge_and_process_movies,
    dag=dag,
)



google_cloud_storage_upload_task = PythonOperator(
    task_id="google_cloud_storage_upload",
    python_callable=google_cloud_storage_upload,
    provide_context=True,
    dag=dag,
)


# 오퍼레이터 정의 순서 설정
cgv_scraper_task >> mega_scraper_task >> netflix_scraper_task >> merge_and_process_movies>> google_cloud_storage_upload_task

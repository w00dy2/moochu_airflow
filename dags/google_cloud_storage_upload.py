from pymongo import MongoClient
import requests
from google.cloud import storage
import os
from io import BytesIO
import connection

def upload_image_url_to_gcs(bucket_name, url, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    response = requests.get(url)

    with BytesIO(response.content) as f:
        blob.upload_from_file(f, content_type='image/jpeg')


def upload_movie_poster_to_gcs(all_movies):
    collection_name = "movies2"
    collection = db[collection_name]

    # 연결된 도큐먼트들에서 필요한 정보들 수집
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './river-dynamo-393506-f834b1182f86.json'

    # 수집한 정보를 이용하여 이미지 파일을 Google Cloud Storage에 업로드합니다.
    for movie in all_movies:
        movie_id = movie['movie_id']
        poster_image_url = movie['poster_img']
        bucket_name = 'end_moochu'
        destination_name = f'{str(movie_id)}.jpg'
        upload_image_url_to_gcs(bucket_name, poster_image_url, destination_name)
        print(destination_name)

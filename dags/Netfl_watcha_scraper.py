from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import requests
from bs4 import BeautifulSoup
import pymongo
import re
import connection

# URLs for Watcha and Netflix



def fetch_movies_data(url):
    base_url = "https://movie.daum.net"
    
    response = requests.get(url)
    html_content = response.content
    soup = BeautifulSoup(html_content, 'html.parser')
    movie_list = soup.find('ul', class_='list_movieranking aniposter_ott')
    
    movies = []
    if movie_list is not None:
        for movie in movie_list.find_all('li'):
            href = movie.find('a', class_='thumb_item')['href']
            full_href = base_url + href
            movieId = href.split('=')[-1]
            api_url = f"{base_url}/api/movie/{movieId}/main"
            
            
            num = movie.find('span', class_='txt_num').text

            # Convert the num to the desired date format
            try:
                released_At = datetime.strptime(num, "%y.%m.%d").strftime("%Y-%m-%d")
            except ValueError:
                released_At = None
            
            title = movie.find('a', class_='link_txt').text
            poster = movie.find('img', class_='img_thumb')['src']
            
            # Determine OTT based on the URL
            ott = 'Unknown'
            if 'watcha' in url:
                ott = 'Watcha'
            elif 'netflix' in url:
                ott = 'Netflix'
            
            moochu = re.sub(r'[^\w\s]', '',title).replace(' ', '')
            
            if released_At is not None:
                movie_data = {
                    'title_kr': title,
                    'moochu' : moochu,
                    'href': api_url,
                    'released_At': released_At,
                    'poster_image_url': poster,
                    'OTT': [ott]  # Add the 'ott' field to the movie data
                }

                movies.append(movie_data)
            
    return movies

def update_movies_data(movies):
    for movie in movies:
        response = requests.get(movie['href'])
        movie_info = response.json()['movieCommon']
        
        # Fetch required data from the JSON response
        title_english = movie_info['titleEnglish']
        plot = movie_info['plot']
        genres = movie_info['genres']
        
        # Find admission_code for Korea
        admission_code = None
        for info in movie_info['countryMovieInformation']:
            if info['country']['nameEnglish'] == 'KOREA':
                admission_code = info['admissionCode']
                break
        
        # Map 'admissionCode' to the desired values
        if admission_code == '15세이상관람가':
            movie['rating'] = 'OVER15'
        elif admission_code == '12세이상관람가':
            movie['rating'] = 'OVER12'
        elif admission_code == '전체관람가':
            movie['rating'] = 'ALL'
        else:
            movie['rating'] = 'UNKNOWN'  # Handle cases where admission_code is not one of the expected values
        
        # Update the movie dictionary with the additional information
        movie['titleEnglish'] = title_english
        movie['synopsis'] = plot
        movie['genres'] = genres
        movie['coming'] = 'TRUE'    
        # Remove the 'admissionCode' from the movie dictionary
        movie.pop('admissionCode', None)



urls = [
    'https://movie.daum.net/premovie/watcha?flag=Y',
    'https://movie.daum.net/premovie/netflix?flag=Y',
    ]

def update_movies_for_collections(urls):
    
    collection_name = "movies2"
    collection = db[collection_name]
    Netfl_new_movie=[]

    for url in urls:
        movies = fetch_movies_data(url)
        update_movies_data(movies)

        print(url)
        print(movies)

        if not movies:
            print("Warning: The 'movies' list is empty. Skipping database insertion.")
            continue

        for movie in movies:  # FIX: This line should have been indented
            existing_movie = collection.find_one({'moochu': movie['moochu']})
            if existing_movie:
                if set(movie['OTT']) <= set(existing_movie.get('OTT', [])):
                    continue
                else:
                    unique_OTTs = list(set(movie['OTT']) - set(existing_movie.get('OTT', [])))
                    collection.update_one({'moochu': movie['moochu']}, {'$addToSet': {'OTT': {'$each': unique_OTTs}}})
            else:

                movie['OTT'] = list(set(movie.get('OTT', [])))  # Initialize as an array if not present
                
                # 영화를 삽입하고 결과를 저장합니다.
                insert_result = collection.insert_one(movie)
                
                # 삽입된 문서의 _id 값을 영화 사전에 추가합니다.
                movie['_id'] = insert_result.inserted_id
                
                Netfl_new_movie.append(movie)
    return Netfl_new_movie

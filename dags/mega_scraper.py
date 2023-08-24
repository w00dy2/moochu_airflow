from datetime import datetime, timedelta
import pymongo
import requests
from bs4 import BeautifulSoup as BS
import re
import connection
def fetch_mega_movie():
    url = 'https://www.megabox.co.kr/on/oh/oha/Movie/selectMovieList.do'
    url2 = 'https://www.megabox.co.kr/on/oh/oha/Movie/selectMovieInfo.do'
    payload = {
        "currentPage": "1",
        "recordCountPerPage": "200",
        "pageType": "rfilmDe",
        "ibxMovieNmSearch": "",
        "onairYn": "MSC02",
        "specialType": "",
        "specialYn": "N"
    }
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
    }


    response = requests.post(url, data=payload, headers=headers)
    data = response.json()
    movie_list = data['movieList']
    formatted_movies = []

    movieNo = [] #영화 줄거리 등을 위한 리스트
        
    for movie in movie_list:
        released_date = movie['rfilmDeReal']
        movieNo.append(movie['movieNo'])
        formatted_movie = {
            "movieNo" : movie['movieNo'],
            "title_kr": movie['movieNm'],
            "released_At": datetime.strptime(released_date, "%Y%m%d").strftime("%Y-%m-%d") if released_date else None,
            "poster_image_url": f"https://img.megabox.co.kr/{movie['imgPathNm'][1:]}" if movie.get('imgPathNm') else None, 
            "OTT": ["Mega"],
            "rating": "UNKNOWN",
            "media": "MOVIE",
            "comming": "TRUE",
            "moochu" :  re.sub(r'[^\w\s]', '',movie['movieNm']).replace(' ', '')
            }
        if not formatted_movie['released_At']:
            continue
        
        if movie['admisClassNm'] == '12세이상관람가':
            formatted_movie['rating'] = 'OVER12'
        elif movie['admisClassNm'] == '15세이상관람가':
            formatted_movie['rating'] = 'OVER15'
        elif movie['admisClassNm'] == '18세이상관람가':
            formatted_movie['rating'] = 'OVER18'
        elif movie['admisClassNm'] == '전체관람가':
            formatted_movie['rating'] = 'ALL'
        formatted_movie['synopsis'] = movie['movieSynopCn'].replace("\n", "<br>")
        formatted_movies.append(formatted_movie)
        
        
    # 영화장르, 등 데이터 
    total2 = []
    for code in movieNo:
        payload3 = {"rpstMovieNo": code}
        r = requests.post(url2, data=payload3)
        info = BS(r.text).select('div.line p')
        info = BS(r.text).select('div.movie-info p')
        movieDic = {
            'movieNo': code,
            'directors': "",
            'genres': "",
            'actors': "",
            'coming':"TRUE"
        }
        
        for i in info:
            direc = []
            genre = []
            actor = []
            if '감독' in i.text:
                for a in i.text.split(':')[1].split(','):
                    direc.append(a.strip())
                movieDic['directors'] = direc
            if '장르' in i.text:
                for a in i.text.split(':')[1].split('/')[0].split(','):
                    genre.append(a.strip())
                movieDic['genres'] = genre
            if '출연진' in i.text:
                for x in i.text.split(':')[1].split(','):
                    actor.append(x.strip())
                movieDic['actors'] = actor
        total2.append(movieDic)
        
    # movieNo로 두 데이터를 비교 후 merge
    merged_movies = []
    for movie1 in total2:
        for movie2 in formatted_movies:
            if movie1['movieNo'] == movie2['movieNo']:
                merged_movie = {**movie1, **movie2}
                merged_movies.append(merged_movie)
                break
    return merged_movies

# db 저장여부 확인후 db 적재
            
def save_mega_mongo(merged_movies):
    collection_name = "movies2"
    collection = db[collection_name]

    for movie in merged_movies:  # 메가박스 정보 2개 합친 것.
        existing_movie = collection.find_one({'moochu': movie['moochu']})
        mega_new_movie=[]
        if existing_movie:
            if set(movie['OTT']) <= set(existing_movie.get('OTT', [])):
                continue
            else:
                unique_OTTs = list(set(movie['OTT']) - set(existing_movie.get('OTT', [])))
                collection.update_one({'moochu': movie['moochu']}, {'$addToSet': {'OTT': {'$each': unique_OTTs}}})
        else:
            movie['OTT'] = list(set(movie.get('OTT', [])))  # Initialize as an array if not present
            collection.insert_one(movie)

            insert_result = collection.insert_one(movie)
            
            # 삽입된 문서의 _id 값을 영화 사전에 추가합니다.
            movie['_id'] = insert_result.inserted_id
            mega_new_movie.append(movie)
    return mega_new_movie
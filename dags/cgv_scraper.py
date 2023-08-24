from datetime import datetime
import requests
from bs4 import BeautifulSoup as bs
import pymongo
import re
import connection
def fetch_cgv_movies():
    url = 'http://www.cgv.co.kr/movies/pre-movies.aspx'
    r = requests.get(url)
    CGv = bs(r.text, 'html.parser')
    cgv = CGv.select_one('div.sect-movie-chart')
    total = []
    linklist = []
    for i in cgv.select('ol li'):
        linklist.append(i.select('a')[0]['href'])
    for a in linklist:
        cgvDic = {}
        new_url = 'http://www.cgv.co.kr' + a
        r = requests.get(new_url)
        info = bs(r.text, 'html.parser')
        # 제목
        cgvDic['title_kr'] = info.select_one('div.box-contents strong').text
        cgvDic['moochu'] = re.sub(r'[^\w\s]', '',cgvDic['title_kr']).replace(' ', '')
        # 개봉일자 2023.08과 같은 형식이면 None. 이후에 제외
        released_date = info.select('div.spec dd')[5].text
        try:
            cgvDic['released_At'] = datetime.strptime(released_date, "%Y.%m.%d").strftime("%Y-%m-%d")
        except ValueError:
            cgvDic['released_At'] = None
        # 포스터이미지
        img = info.select_one('div.box-image img')['src']
        cgvDic['poster_image_url'] = img
        # OTT 정보
        cgvDic['OTT'] = ['CGV']
        cgvDic['coming'] ='TRUE'        
        # 감독정보
        if info.select('div.spec dl dt')[0].text[0] == '감':
            g = []
            for i in info.select_one('div.spec dd').text.split(','):
                if i in '\n':
                    i = i.replace('\n', '')
                if i in '\r':
                    i = i.replace('\r', '')
                if i in '\xa0':
                    i = i.replace('\xa0', '')
                g.append(i.strip())
            cgvDic['director'] = g
        # 관람등급
        view_grade_info = info.select('div.spec dl dd.on')[1].text
        view_grade = view_grade_info.split(',')[0].strip()
        if str(view_grade) == '전체관람가':
            cgvDic['rating'] = 'ALL'
        elif str(view_grade) == '12세이상관람가':
            cgvDic['rating'] = 'OVER12'
        elif str(view_grade) == '15세이상관람가':
            cgvDic['rating'] = 'OVER15'
        elif str(view_grade) == '18세이상관람가':
            cgvDic['rating'] = 'OVER18'
        else:
            cgvDic['rating'] = 'UNKNOWN'
        # 배우정보
        if info.select('div.spec dl dt')[1].text.split(' ')[1] == '배우':
            g = []
            for i in info.select('div.spec dd.on a'):
                g.append(i.text)
            cgvDic['actor'] = g
        else:
            cgvDic['actor'] = ""
        # 장르
        if info.select('div.spec dt')[2].text[0] == '장':
            g = []
            genre_values = info.select('div.spec dt')[2].text.replace('\xa0', '').split(':')[1:]
            for i in genre_values:
                g.append(i.strip())
            cgvDic['genres'] = g
        else:
            cgvDic['genres'] = ""
        # 시놉시스
        synopsis_element = info.select_one('div.col-detail div.sect-story-movie')        
        cgvDic['synopsis'] = synopsis_element.text.replace('\n', '<br>').replace('\r', '') if synopsis_element else ""
        # 추가 정보
        info_elements = info.select('div.spec dl dd.on')
        cgvDic['info'] = info_elements[1].text if len(info_elements) > 1 else ""
        # 베우정보
        if info.select('div.spec dl dt')[1].text.split(' ')[1]:
            g = []
            for i in info.select('div.spec dd.on a'):
                g.append(i.text)
            cgvDic['actor'] = g
        else:
            cgvDic['actor'] = ""
        if cgvDic['released_At'] is not None:
            total.append(cgvDic)    
    return total

# db 저장여부 확인후 db 적재
def save_cgv_movies(total):
    collection_name = "movies2"
    collection = db[collection_name]
    cgv_new_movie = []
    for movie in total:  # 메가박스 정보 2개 합친 것.
        existing_movie = collection.find_one({'moochu': movie['moochu']})
        if existing_movie:
            if set(movie['OTT']) <= set(existing_movie.get('OTT', [])):
                continue
            else:
                unique_OTTs = list(set(movie['OTT']) - set(existing_movie.get('OTT', [])))
                collection.update_one({'moochu': movie['moochu']}, {'$addToSet': {'OTT': {'$each': unique_OTTs}}})
        else:
            movie['OTT'] = list(set(movie.get('OTT', [])))  # Initialize as an array if not present
            insert_result = collection.insert_one(movie)
            
            # 삽입된 문서의 _id 값을 영화 사전에 추가합니다.
            movie['_id'] = insert_result.inserted_id
            cgv_new_movie.append(movie)
            
    return cgv_new_movie            
            

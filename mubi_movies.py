#!/usr/bin/env python
# coding: utf-8


import requests
import json
import pandas as pd
from datetime import date





today = date.today()
print("Data extraction date:", today)



def mubi_movie_function(expected_number_of_movies_per_page, first_page, last_page):
    
    for page in range(first_page, last_page + 1):
        
        print(page)
                    
        movie_json_data = requests.get('https://mubi.com/services/api/films?page=' + str(page)).json()
            
        number_of_movies_per_page = len(movie_json_data)
        
        movie_id_list = []
        movie_url_list = []
        movie_title_list = []
        movie_title_language_list = []
        movie_release_year_list = []
        movie_popularity_list = []
        movie_image_url_list = []
        director_name_list = []
        director_url_list = []
        director_id_list = []
        
        for movie in movie_json_data:
            
            movie_id_list.append(movie['id'])
            movie_url_list.append(movie['canonical_url'])
            movie_title_list.append(movie['title'])
            movie_title_language_list.append(movie['title_locale'])
            movie_release_year_list.append(movie['year'])
            movie_popularity_list.append(movie['popularity'])
            movie_image_url_list.append(movie['still_url'])
            
            
            
            if len(movie['directors']) != 1:
                
                for director_number in range(len(movie['directors'])):
                    
                    if director_number == 0:
                        director_name = movie['directors'][director_number]['name']
                        director_url = movie['directors'][director_number]['canonical_url']
                        director_id = str(movie['directors'][director_number]['id'])
                    else:
                        director_name = director_name + ', ' + movie['directors'][director_number]['name']
                        director_url = director_url + ', ' + movie['directors'][director_number]['canonical_url']
                        director_id = director_id + ', ' + str(movie['directors'][director_number]['id'])
            
            else:
                director_name = movie['directors'][0]['name']
                director_url = movie['directors'][0]['canonical_url']
                director_id = str(movie['directors'][0]['id'])
                
            
            director_name_list.append(director_name)
            director_url_list.append(director_url)
            director_id_list.append(director_id)
            
        
        movie_data = pd.DataFrame.from_dict({'movie_id': movie_id_list, 'movie_url': movie_url_list,
                                             'movie_title': movie_title_list, 'movie_title_language': movie_title_language_list,
                                             'movie_release_year': movie_release_year_list, 'movie_popularity': movie_popularity_list,
                                             'movie_image_url': movie_image_url_list, 'director_name': director_name_list, 
                                             'director_url': director_url_list, 'director_id': director_id_list})
        
        if number_of_movies_per_page == expected_number_of_movies_per_page:
            
            if page == first_page:
            
                movie_full_data = movie_data.copy()
            
            else:
            
                movie_full_data = pd.concat([movie_full_data, movie_data]) 
                
                if page == last_page:
                    
                    movie_full_data = movie_full_data[['movie_id', 'movie_title', 'movie_release_year',
                                                  'movie_url', 'movie_title_language', 'movie_popularity',
                                                  'movie_image_url', 'director_id', 'director_name',
                                                  'director_url']]
                    
                    return movie_full_data
                
        else:
            
            movie_full_data = pd.concat([movie_full_data, movie_data])
            
            movie_full_data = movie_full_data[['movie_id', 'movie_title', 'movie_release_year',
                                                  'movie_url', 'movie_title_language', 'movie_popularity',
                                                  'movie_image_url', 'director_id', 'director_name',
                                                  'director_url']]
        
            return movie_full_data

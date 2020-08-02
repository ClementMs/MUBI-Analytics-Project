
# coding: utf-8


import requests
import json
import pandas as pd
from datetime import date
from json import JSONDecodeError



def mubi_rating_function(number_of_ratings_per_request, movie_id_list):
    
    movie_errors = []
    
    s = requests.Session()
    
    movie_counter = 0
        
    for movie_id in movie_id_list:
        
        if (movie_counter == 0) or (movie_counter == 1):
            
            movie_counter += 1
                
        print(movie_id)
        
        page = 1
        
        number_of_ratings_per_page = number_of_ratings_per_request
    
        while number_of_ratings_per_page != 0:
            
            try:
                 
                rating_json_data = s.get('https://mubi.com/services/api/ratings?film_id=' 
                                            + str(movie_id) + '&page=' + str(page) 
                                            + '&per_page=' + str(number_of_ratings_per_request)).json()
            
                number_of_ratings_per_page = len(rating_json_data)            
            
            
                rating_data = pd.DataFrame.from_dict({'rating_json_data': rating_json_data, 
                                                      'movie_id': movie_id, 'page': page})
                    
                if (page == 1) and (movie_counter == 1):
            
                    rating_full_data = rating_data.copy()
            
                else:
            
                    rating_full_data = pd.concat([rating_full_data, rating_data]) 
            
                page += 1
            
            except TypeError:
                pass
            
            except JSONDecodeError:
                pass
                    
    rating_full_data = rating_full_data.reset_index()[['movie_id', 'page', 'rating_json_data']]
    
    rating_full_data['rating_id'] = rating_full_data['rating_json_data'].apply(lambda x: x['id'])
                    
    return rating_full_data



def processing_ratings_json_data(data):
    
    data['rating_url'] = data['rating_json_data'].map(lambda x: dict(eval(x))).apply(lambda x: x['canonical_url'])
    data['rating_score'] = data['rating_json_data'].map(lambda x: dict(eval(x))).apply(lambda x: x['overall'])
    data['rating_timestamp'] = data['rating_json_data'].map(lambda x: dict(eval(x))).apply(lambda x: x['updated_at'])
    data['critic'] = data['rating_json_data'].map(lambda x: dict(eval(x))).apply(lambda x: x['body'])
    data['critic_likes'] = data['rating_json_data'].map(lambda x: dict(eval(x))).apply(lambda x: x['like_count'])
    data['critic_comments'] = data['rating_json_data'].map(lambda x: dict(eval(x))).apply(lambda x: x['comment_count'])
    data['user_id'] = data['rating_json_data'].map(lambda x: dict(eval(x))).apply(lambda x: x['user_id'])
    data['user_trialist'] = data['rating_json_data'].map(lambda x: dict(eval(x))).apply(lambda x: x['user']['trialist'] if type(x['user'])== dict else np.nan)
    data['user_subscriber'] = data['rating_json_data'].map(lambda x: dict(eval(x))).apply(lambda x: x['user']['subscriber'] if type(x['user'])== dict else np.nan)
    data['user_url'] = data['rating_json_data'].map(lambda x: dict(eval(x))).apply(lambda x: x['user']['canonical_url'] if type(x['user'])== dict else np.nan)
    data['user_avatar_image_url'] = data['rating_json_data'].map(lambda x: dict(eval(x))).apply(lambda x: x['user']['avatar_url'] if type(x['user'])== dict else np.nan)
    data['user_cover_image_url'] = data['rating_json_data'].map(lambda x: dict(eval(x))).apply(lambda x: x['user']['cover_image_url'] if type(x['user'])== dict else np.nan)
    data['user_eligible_for_trial'] = data['rating_json_data'].map(lambda x: dict(eval(x))).apply(lambda x: x['user']['eligible_for_trial'] if type(x['user'])== dict else np.nan)
    data['user_has_payment_method'] = data['rating_json_data'].map(lambda x: dict(eval(x))).apply(lambda x: x['user']['has_payment_method'] if type(x['user'])== dict else np.nan)
    
    data = data[['movie_id', 'rating_id', 'rating_url',
       'rating_score', 'rating_timestamp', 'critic', 'critic_likes',
       'critic_comments', 'user_id', 'user_trialist', 'user_subscriber',
       'user_url', 'user_avatar_image_url', 'user_cover_image_url',
       'user_eligible_for_trial', 'user_has_payment_method']]
    
    return data

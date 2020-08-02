#!/usr/bin/env python
# coding: utf-8


import requests
import json
import pandas as pd
import numpy as np
from datetime import date
from json import JSONDecodeError



def mubi_list_function(number_of_lists_per_request, user_id_list):
        
    s = requests.Session()
    
    user_counter = 0
        
    for user_id in user_id_list:
                
        if (user_counter == 0) or (user_counter == 1):
            
            user_counter += 1
                        
        page = 1
        
        number_of_lists_per_page = number_of_lists_per_request
    
        while number_of_lists_per_page != 0:
            
            try:
                 
                list_json_data = s.get('https://mubi.com/services/api/lists?user_id=' 
                                            + str(user_id) + '&page=' + str(page) 
                                            + '&per_page=' + str(number_of_lists_per_request)).json()
            
                number_of_lists_per_page = len(list_json_data)            
            
            
                list_data = pd.DataFrame.from_dict({'list_json_data': list_json_data, 
                                                      'user_id': user_id, 'page': page})
                    
                if (page == 1) and (user_counter == 1):
            
                    list_full_data = list_data.copy()
            
                else:
            
                    list_full_data = pd.concat([list_full_data, list_data]) 
            
                page += 1
            
            except TypeError:
                pass
            
            except JSONDecodeError:
                pass
                    
    list_full_data = list_full_data.reset_index()[['user_id', 'page', 'list_json_data']]
                            
    return list_full_data



def processing_lists_json_data(data):
    
    for element in data['list_json_data'].values:
        for key in element.keys():
            if element[key] == None: 
                element[key] = np.nan
    
    data['list_id'] = data['list_json_data'].apply(lambda x: x['id'])
    data['list_title'] = data['list_json_data'].apply(lambda x: x['title'])
    data['list_movie_number'] = data['list_json_data'].apply(lambda x: x['list_films_count'])
    data['list_update_timestamp'] = data['list_json_data'].apply(lambda x: x['updated_at'])
    data['list_creation_timestamp'] = data['list_json_data'].apply(lambda x: x['created_at'])
    data['list_followers'] = data['list_json_data'].apply(lambda x: x['fanship_count'])
    data['list_url'] = data['list_json_data'].apply(lambda x: x['canonical_url'])
    data['list_comments'] = data['list_json_data'].apply(lambda x: x['comment_count'])
    data['list_description'] = data['list_json_data'].apply(lambda x: x['description'])
    data['list_cover_image_url'] = data['list_json_data'].apply(lambda x: np.nan if type(x['image_urls'])==float else x['image_urls']['large'])
    data['list_first_image_url'] = data['list_json_data'].apply(lambda x: x['thumbnail_urls'][0] if (type(x['thumbnail_urls'] == list) and len(x['thumbnail_urls'])>=1) else np.nan)
    data['list_second_image_url'] = data['list_json_data'].apply(lambda x: x['thumbnail_urls'][1] if (type(x['thumbnail_urls'] == list) and len(x['thumbnail_urls'])>=2) else np.nan)
    data['list_third_image_url'] = data['list_json_data'].apply(lambda x: x['thumbnail_urls'][2] if (type(x['thumbnail_urls'] == list) and len(x['thumbnail_urls'])>=3) else np.nan)
    data['user_trialist'] = data['list_json_data'].apply(lambda x: x['user']['trialist'] if type(x['user'])== dict else np.nan)
    data['user_subscriber'] = data['list_json_data'].apply(lambda x: x['user']['subscriber'] if type(x['user'])== dict else np.nan)
    data['user_url'] = data['list_json_data'].apply(lambda x: x['user']['canonical_url'] if type(x['user'])== dict else np.nan)
    data['user_avatar_image_url'] = data['list_json_data'].apply(lambda x: x['user']['avatar_url'] if type(x['user'])== dict else np.nan)
    data['user_cover_image_url'] = data['list_json_data'].apply(lambda x: x['user']['cover_image_url'] if type(x['user'])== dict else np.nan)
    data['user_eligible_for_trial'] = data['list_json_data'].apply(lambda x: x['user']['eligible_for_trial'] if type(x['user'])== dict else np.nan)
    data['user_has_payment_method'] = data['list_json_data'].apply(lambda x: x['user']['has_payment_method'] if type(x['user'])== dict else np.nan)
    
    data = data[['user_id','list_id', 'list_title', 'list_movie_number',
                'list_update_timestamp', 'list_creation_timestamp',
                'list_followers', 'list_url', 'list_comments',
                'list_description', 'list_cover_image_url','list_first_image_url',
                'list_second_image_url', 'list_third_image_url',
                'user_trialist', 'user_subscriber', 'user_url',
                'user_avatar_image_url', 'user_cover_image_url',
                'user_eligible_for_trial', 'user_has_payment_method']]
    
    return data


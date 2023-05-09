# -*- coding: utf-8 -*-
"""
Created on Mon May  8 17:39:28 2023

@author: segur
"""

import pandas as pd
import random
import hashlib
from magi_core import magi_connections as conn

def creates_pwd():
    letters = [
        'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o',
        'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', 'A', 'B', 'C', 'D',
        'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S',
        'T', 'U', 'V', 'W', 'X', 'Y', 'Z'
    ]
    
    pwd = ''

    for char in range(8):
        pwd += random.choice(letters)
    
    
    m = hashlib.sha256()
    m.update(pwd.encode())
    
    pwd = m.hexdigest()
    
    return pwd


def get_usuarios():
    soql = """
        SELECT 
            Id, 
            Name,
            Email, 
            VEC_Campus_de_Usuario__c, 
            Profile.Name, 
            VEC_Zona_Regional__c, 
            Manager.Email, 
            IsActive, 
            CreatedDate  
        FROM User 
        WHERE Profile.name LIKE '%U4P%'
    """
    sf = conn.retry_conect_sf()
    res = pd.DataFrame(sf.query_all(soql)['records'])
    
    res['Funcion'] = res['Profile'].apply(lambda x: x['Name'] if not pd.isnull(x) else None)
    res['Manager'] = res['Manager'].apply(lambda x: x['Email'] if not pd.isnull(x) else None)
    del res['Profile']
    del res['attributes']
    
    res['CreatedDate'] = pd.to_datetime(res['CreatedDate']).dt.tz_convert('America/Chihuahua').dt.tz_localize(None)
    return res


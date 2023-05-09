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
    
    numbers = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']
    
    
    password_list = []

    for char in range(8):
        password_list.append(random.choice(letters))
    
    for char in range(2):
        password_list.append(random.choice(numbers))
    
    
    random.shuffle(password_list)

    pwd = ""
    for char in password_list:
        pwd += char
    return pwd


def encrypt_pwd(pwd):
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
    
    res['rol'] = res['Profile'].apply(lambda x: x['Name'] if not pd.isnull(x) else None)
    res['manager'] = res['Manager'].apply(lambda x: x['Email'] if not pd.isnull(x) else None)
    
    res['CreatedDate'] = pd.to_datetime(res['CreatedDate']).dt.tz_convert('America/Chihuahua').dt.tz_localize(None)
    
    res['CreatedDate'] = res['CreatedDate'].apply(lambda x:  x.strftime("%d/%m/%Y %H:%M"))
    
    
    res.rename(columns={
        'Id' : 'sf_id',
        'Name' : 'name',
        'Email' : 'email',
        'VEC_Campus_de_Usuario__c' : 'campus',
        'VEC_Zona_Regional__c' : 'region',
        'CreatedDate' : 'created_at'
        }, inplace=True)
    
    
    del res['Profile']
    del res['attributes']
    del res['Manager']
    del res['IsActive']

    
    return res.copy()


#get bases
base = pd.read_csv(r'resources/users.csv')
users = conn.get_sf_users()
users_sf = get_usuarios()


# ACTIVOS
users = users[users['Activo'] == '1']
users['en Asignator'] = users['Correo del asesor'].isin(base['email'])

users_sf['add'] = users_sf['email'].isin(users['Correo del asesor'][~users['en Asignator']])
users_sf = users_sf[(users_sf['add']) & (users_sf['rol'] == 'VEC Asesor U4P')]
users_sf['pwd'] = users_sf.apply(lambda x : creates_pwd(), axis=1)


#copy and paste
users_add = users_sf.copy()
users_add['pwd'] = users_add['pwd'].apply(lambda x : encrypt_pwd(x))

del users_add['add']

users_add = users_add.to_json(orient="records", force_ascii=False)


for ix, row in users_sf.iterrows():
    print(f"user : {row['email']} - manager: {row['manager']} - pass: {row['pwd']}")



# TURN OFF 
users_sf = get_usuarios()
base['activo en magi'] = base['email'].isin(users['Correo del asesor'])


base_off = base[~(base['activo en magi']) & (base['region'] != 'Región VEC') & (pd.isna(base['deleted_at']))].copy()
base_off['new_pwd'] = "Inactivo" 
base_off['new_pwd'] = users_sf['new_pwd'].apply(lambda x : encrypt_pwd(x))


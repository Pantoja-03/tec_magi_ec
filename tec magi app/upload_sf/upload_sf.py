import pandas as pd
from tqdm import tqdm
from magi_core import magi_connections as conn
import requests
import time

url_teams = conn.get_url_teams()


def upload_lead(lead, sf):
    error = None
    for i in range(2):
        time.sleep(1)
        try:
            return sf.Lead.create(lead), True
            break
        
        except Exception as e:
            error = e
    
    return error, False
    
    
    
def upload_bulk(base_to_sf, base_to_sf_dict):
    sf = conn.retry_conect_sf()
    res = sf.bulk.Lead.insert(base_to_sf_dict)

    #join
    base_ = []
    for row in range(len(base_to_sf)):
        base_.append({**base_to_sf[row], **res[row]})
    
    base_to_sf = pd.DataFrame(base_)
    
    if base_to_sf['id'].count() == 0:
        raise Exception(base_to_sf.iloc[0]['errors'][0]['message'])
    
    return base_to_sf.copy()


def upload_one_to_one(base_to_sf, base_to_sf_dict):
    sf = conn.retry_conect_sf()
    ix = 0
        
    for lead in tqdm(base_to_sf_dict):
    
        res, r = upload_lead(lead, sf)
           
        if not r:
            res = {
                    'errors' : res,
                    'id' : None,
                    'success' : False
                    }
            
        base_to_sf[ix].update(res)
        ix += 1
        
    base_to_sf = pd.DataFrame(base_to_sf)
    return base_to_sf.copy()


def upload(base: pd.DataFrame, base_to_sf: pd.DataFrame, use_bulk = True):

    base = base.fillna('')
    base_to_sf = base_to_sf.fillna('')
    base['errors'] = ''

    base_to_sf_dict = base_to_sf[[
        'RecordTypeId',
        'FirstName',
        'LastName',
        'Status',
        'VEC_Preferencia_Contacto__c',
        'Email',
        'MobilePhone',
        'Phone',
        'VEC_Programa__c',
        'VEC_Modalidad__c',
        'VEC_CampusSede__c',
        'Company',
        'VEC_Origen_Prospecto__c',
        'Campana_Origen__c',
        'OwnerId',
        'Description',
        'VEC_id_Marketing__c'
    ]].copy()

    #transfomr to dict
    base_to_sf = base_to_sf.to_dict('records')
    base_to_sf_dict = base_to_sf_dict.to_dict('records')

    
    #loading to SF
    print("\n.........")
    print("\nCargando datos a SF...")
    
    if use_bulk:
        #first try bulk
        try:
           base_to_sf = upload_bulk(base_to_sf, base_to_sf_dict)
        
        except:
            msg = 'Fallo la carga por bulk en iup ec.'
            requests.post(url_teams, json={'text': msg})
            
            base_to_sf = upload_one_to_one(base_to_sf, base_to_sf_dict)
            
        
    else:
        base_to_sf = upload_one_to_one(base_to_sf, base_to_sf_dict)



    base_to_sf = base_to_sf.set_index('Id')
    
    base_valid = base[base['status'] == 'Contacto Nuevo']
    base = base[base['status'] != 'Contacto Nuevo']

    base_valid['sf_id'] = base_valid['id'].map(base_to_sf['id'])
    base_valid['errors'] = base_valid['id'].map(base_to_sf['errors'])
    
    
    if base_to_sf['id'].count() != len(base_to_sf['id']):    
        msg = "Hay: " + str(len(base_to_sf['id']) - base_to_sf['id'].count()) +" leads que no se puedieron cargar."   

        #Send warning
        requests.post(url_teams, json={'text': msg})
        base_to_sf['Status'] = base_to_sf.apply(lambda row: "Pendiente en SF" if row['id'] is None else row['Status'],axis=1)
        base_valid['status'] = base_valid['id'].map(base_to_sf['Status'])
        print("\nHay registros que no se pudieron cargar.")
        print(".........\n")

    else:
        print("Registros cargados correctamente a SF.")
        print(".........\n")
        
    base = pd.concat([base, base_valid])   

    return base.copy()

    

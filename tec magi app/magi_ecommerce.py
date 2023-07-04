
import pandas as pd
from magi_core import magi_connections as magi_conn
from magi_core import magi_validate
from magi_core import magi_assignment
from datetime import timedelta, date
import requests
import os
from dotenv import load_dotenv
import time
from tqdm import tqdm

load_dotenv()
url_teams = os.getenv('URL_TEAMS')


def assignment_eccomerce(lead, data, sf):
    error = None
    for i in range(3):
        time.sleep(1)
        try:
            return sf.Opportunity.update(lead, data), True
            break
        
        except Exception as e:
            error = e
    
    return error, False

def upload_one_to_one(base_to_sf):
    sf = magi_conn.retry_conect_sf()
        
    for lead in tqdm(base_to_sf):
        res, r = assignment_eccomerce(lead, base_to_sf[lead], sf)
           
        if r == False:
            res = {
                'errors' : res,
                'id' : None,
                'success' : False
                }
        else:
            res = {
                'errors' : None,
                'id' : lead,
                'success' : True
                }
            
        base_to_sf[lead].update(res)
        
    base_to_sf = pd.DataFrame.from_dict(base_to_sf, orient='index')
    
    return base_to_sf.copy()

try:
    #GET BASE
    print("\n.........")
    print("\nLeyendo base de Ecommerce...")
    base =  magi_conn.get_base_ecommerce()
    base['start_date'] = pd.to_datetime(base['start_date'])
    base_old = base[base['start_date'].dt.date < date.today()]
    base = base[base['start_date'].dt.date >= date.today()]
    
    if len(base) > 0:
    
        #VALIDATE BASE
        print("\n.........")
        print("\nValidando datos...")
        base, sf_leads = magi_validate.validate(base)
        
        #VALIDATE DUPLICATES
        sf_leads = sf_leads[sf_leads['OwnerId'] != '0053f000000Fe2SAAS']
        
        base['KEY'] = base.apply(lambda row: str(row['email']).upper().strip() + str(row['VEC_Programa__c']).upper().strip(),axis=1)
        base['duplicated_sf'] = False
        base['recidivist'] = base['email_corrected'].isin(sf_leads['Email'])
        base['status'] = 'Contacto Nuevo'
        
        
        
        #APPLY ASSIGNMENT RULES
        print("\n.........")
        print("\nAplicando reglas de asignacion...")
        base = magi_assignment.assignment(base, sf_leads)
        
        
        
        #UPDATE IN SF
        print("\n.........")
        print("\nCargando datos a SF...")
        base = base.fillna('')
        base_to_sf = base[['id','owner_id']].rename(columns={'id':'Id', 'owner_id':'OwnerId'}).copy()
        base['errors'] = ''
        
        #transfomr to dict
        base_to_sf = base_to_sf.set_index('Id').to_dict('index')
        base_to_sf = upload_one_to_one(base_to_sf)
        
        
        base['errors'] = base['id'].map(base_to_sf['errors'])
        
        
        if len(base[~pd.isna(base['errors'])]) > 0:    
            msg = f"Hay {len(base[~pd.isna(base['errors'])])} oportunidades que no se puedieron asignar."   
            requests.post(url_teams, json={'text': msg})
            
            print("\n" + msg)
        
        else:
            msg = f"Se asignaron {len(base)} oportunidades."   
            requests.post(url_teams, json={'text': msg})
            
            print("\n" + msg)
        
        
        print(".........\n")
        
except Exception as e:
    msg = 'No se pudo procesar la asignacion de ecommerce. Error:\n\n\t' + str(e)
    requests.post(url_teams, json={'text': msg}) 

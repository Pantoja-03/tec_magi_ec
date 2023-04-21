import pandas as pd
import requests
import upload_sf.base_to_sf as to_sf
import upload_sf.upload_sf as upload_sf
import upload_sf.document_sf as document_sf
import upload_sf.document_activity_new_leads as document_new_leads  
from magi_core import magi_connections as conn

url_teams = conn.get_url_teams()


def upload(base: pd.DataFrame, use_bulk):
    #get base
    base_to_sf = to_sf.get_base(base)
    
    #upload to sf
    base = upload_sf.upload(base, base_to_sf, use_bulk)
    
    #documents duplicates in sf
    try:
        document_sf.document(base)
        
        document_new_leads.document(base)
    
    except:
        requests.post(url_teams, json={'text': "Error al documentar las actividades en la carga de los leads U4P"})
    
    
    return base.copy()
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 17 11:28:33 2023

@author: segur
"""

import pandas as pd
import requests
import json
import time
from magi_core import magi_connections as magi_conn

url_teams = magi_conn.get_url_teams()


def send_to_asignator(base: pd.DataFrame):
    
    base_a = base[base['status'] == 'Cargado']    
    
    try:
        if len(base) > 0:
            base_aux = base_a.copy()
            base_aux['manual'] = False
            limit_n = 20
            
            df_iter = []
            while len(base_aux) > 0:
                df_iter.append(base_aux[:limit_n])
                
                base_aux = base_aux[limit_n:]
            
            
            
            for df in df_iter:
                #ENVIO A ASIGNATOR
                headers = { 'Content-Type' : 'application/json'}
                        
                api_url = 'https://asignator.tecdap.net/api/v1/bulk/lead'
                base_assignator = df[['sf_id','loaded_phone', 'name', 'owner', 'created_at', 'processing_date', 'assignment_type', 'manual']].rename(columns={'loaded_phone':'phone'})  
                
                base_assignator['created_at'] = pd.to_datetime(base_assignator['created_at'].str[:19])
                base_assignator['created_at'] = base_assignator['created_at'].dt.tz_localize('America/Monterrey').dt.tz_convert('UTC').astype(str).str.replace(' ', 'T')
                base_assignator['processing_date'] = pd.to_datetime(base_assignator['processing_date'].str[:19])
                base_assignator['processing_date'] = base_assignator['processing_date'].dt.tz_localize('America/Monterrey').dt.tz_convert('UTC').astype(str).str.replace(' ', 'T')
                
                
                leads = {"leads": base_assignator.to_dict(orient='records')}
                leads = json.dumps(leads)
                
                r = requests.post(url = api_url, data = leads, headers=headers, timeout=30)
                
                
                if r.text != '{"status":"OK"}':
                    msg = 'No se pudo mandar el post a Asignator :('
                    requests.post(url_teams, json={'text': msg})
                    
                
                time.sleep(1)
            
            
            url = "https://asignator.tecdap.net/api/v1/c_sessions"
            payload={}
            headers = {'Authorization': 'Bearer 643f0ee9c363f005c736e540'}
            response = requests.request("GET", url, headers=headers, data=payload)
            asesores_activos = (response.text).replace('[','').replace('"','').replace(']','').split(',')
            
            
            #dict_to_asignator = {
            #    'leads' : base_a,
            #    'asesores_activos' : asesores_activos
            #    }
            
            #name = str(datetime.now()).replace(".","").replace(":","")
            
            # pd.DataFrame().to_pickle(f'./resources/asignator_logs/{name}.pkl')
            
            # with open(f'./resources/asignator_logs/{name}.pkl', 'wb') as handle:
            #     pickle.dump(dict_to_asignator, handle, protocol=pickle.HIGHEST_PROTOCOL)
                
                
            print("Leads enviados a asignator")
            
        return True
    
    except:
        pass
         
     
     
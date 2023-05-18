import pandas as pd
import numpy as np
import magi_core.magi_connections as conn
from unidecode import unidecode

def get_campus_catalogs():
    #Get program catalogs
    try:
        campus_catalogs = conn.get_campus_catalogs()
    except:
        campus_catalogs = pd.read_pickle('./resources/catalogos/campus_catalogs.pkl')
        campus_catalogs = campus_catalogs.set_index('ALIAS')

    return campus_catalogs.copy()



def validate(base: pd.DataFrame): 
    campus_catalogs = get_campus_catalogs()
    
    base['ALIAS'] = base['campus'].apply(lambda x: unidecode(x).upper().replace('  ',' ').replace('—','-').strip())

    #add region
    base["region"] = base['ALIAS'].map(campus_catalogs['Region'])
    base["loaded_campus"] = base['ALIAS'].map(campus_catalogs['Campus Salesforce'])
    base['VEC_CampusSede__c'] = base['ALIAS'].map(campus_catalogs['Id Salesforce'])

    base['valid_campus'] = ~base['loaded_campus'].isin([np.nan, ""])

    #Change original campus
    base['campus'] = base.apply(lambda row: row["loaded_campus"] if row["valid_campus"] == True else row['campus'], axis=1)

    #Apply modality online
    base.loc[base.modality != "Presencial", 'loaded_campus'] = "Programas en Línea"
    base.loc[base.modality != "Presencial", 'VEC_CampusSede__c'] = "0012M00002EDbuY"
    base.loc[base.modality != "Presencial", 'region'] = "Región en Línea"
    base.loc[base.modality != "Presencial", 'valid_campus'] = True
    
    #parchamos a la region latam
    base['es latam?'] = False
    base['es latam?'] = base.apply(lambda row: True if (row['loaded_phone'][:2] not in ['52', '']) and (row['modality'] != 'Learning Gate')  else False,axis=1)
    
    #base.loc[base['es latam?'], 'loaded_campus'] = "Campus LATAM"
    #base.loc[base['es latam?'], 'VEC_CampusSede__c'] = "0013f000002GaOA"
    base.loc[base['es latam?'], 'region'] = "Región Internacional"
    base.loc[base['es latam?'], 'valid_campus'] = True

    
    #Validate campus
    base['valid_campus'] = ~base['loaded_campus'].isin([np.nan, ""])

    
    del base['ALIAS']
    
    return base.copy()
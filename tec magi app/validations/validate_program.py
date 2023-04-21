import pandas as pd
import numpy as np
import magi_core.magi_connections as conn
from unidecode import unidecode

def get_program_catalogs():
    #Get program catalogs
    try:
        program_catalogs = conn.get_program_catalogs()
    except:
        program_catalogs = pd.read_pickle('./resources/catalogos/program_catalogs.pkl')
        program_catalogs = program_catalogs.set_index('ALIAS')

    return program_catalogs.copy()

def get_program_invalids():
    #Get program invalid catalogs
    try:
        program_invalids = conn.get_program_invalids()
    except:
        program_invalids = pd.read_pickle('./resources/catalogos/program_invalids.pkl')

    return program_invalids.copy()


def validate(base: pd.DataFrame):
    program_catalogs = get_program_catalogs()
    program_invalids = get_program_invalids()

    base['ALIAS'] = base['program_name'].apply(lambda x: unidecode(x).upper().replace('  ',' ').replace('—','-').strip())

    base["loaded_program"] = base['ALIAS'].map(program_catalogs['Programa Salesforce'])
    base['VEC_Programa__c'] = base['ALIAS'].map(program_catalogs['Id Salesforce'])
    base["program_type"] = base['ALIAS'].map(program_catalogs['Tipo de programa'])
    base['field_interest'] = base['ALIAS'].map(program_catalogs['Area tematica'])
    base["school"] = base['ALIAS'].map(program_catalogs['Escuela'])
    base['modality'] = base['ALIAS'].map(program_catalogs['Modalidad'])
   
    base['valid_program'] = ~base['loaded_program'].isin([np.nan, ""])

    #Change original program
    base['corrected_program_name'] = base['program_name'].copy()
    base.loc[base.valid_program, 'corrected_program_name'] =  base.loc[base.valid_program, 'loaded_program']

    #get programs invalids
    programs_invalids = base[['corrected_program_name', 'source']][(~base['corrected_program_name'].isin(program_invalids['Programa'])) & (base['valid_program'] == False) & (base['status'] == 'Contacto Nuevo')].drop_duplicates().copy()
    programs_invalids = programs_invalids[~programs_invalids['corrected_program_name'].isin([np.nan, ""])]


    del base['ALIAS']
    
    return base.copy(), programs_invalids.copy()

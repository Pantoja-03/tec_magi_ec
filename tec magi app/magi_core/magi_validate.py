import validations.validate_sintax as v_sintax
import validations.validate_email as v_email
import validations.validate_phone as v_phone
import validations.validate_program as v_program
import validations.validate_campus as v_campus
import validations.validate_duplicates as v_duplicates
import magi_core.magi_connections as conn

import pandas as pd
import requests
import os
from dotenv import load_dotenv

load_dotenv()
url_teams = os.getenv('URL_TEAMS')


def unique_name(l):
    ulist = []
    [ulist.append(x) for x in l if x not in ulist]
    
    return ulist

def validate_is_test(email, full_name):
    email = email.lower()
    full_name = full_name.lower()
    
    if email.find('test') >= 0 or email.find('prueba') >= 0:
        return True
    
    if full_name.find('test') >= 0 or full_name.find('prueba') >= 0 or full_name.find('prueba.com') >= 0:
        return True
    
    return False

def valid_lead(valid_email, valid_phone, valid_program, valid_campus, test, email_black_list, phone_black_list):
    if valid_email == False and valid_phone == False:
        return False
    
    if valid_program == False or valid_campus == False:
        return False
    
    if test:
        return False
    
    if email_black_list or phone_black_list:
        return False
    
    return True
    
def format_data(base: pd.DataFrame):
    base = base.fillna('')
    base['status'] = 'Contacto Nuevo'

    #format sintaxys
    base['name'] = base.apply(lambda row: v_sintax.format_name(str(row['name'])).title()[:39], axis=1)
    base['last_name1'] = base.apply(lambda row: v_sintax.format_name(str(row['last_name1'])).title()[:39], axis=1)
    base['last_name2'] = base.apply(lambda row: v_sintax.format_name(str(row['last_name2'])).title()[:39], axis=1)
    base['program_type'] = base.apply(lambda row: v_sintax.format_name(str(row['program_type'])), axis=1)
    base['last_name'] = base.apply(lambda x: v_sintax.get_lastName(str(x['last_name1']), str(x['last_name2'])),axis=1)
    base['full_name'] = base.apply(lambda row: str(row['name'] + " " + row['last_name']).strip(),axis=1)
    base['description'] = base.apply(lambda row: v_sintax.format_comments(str(row['description'])), axis=1)
    base['original_lead'] = 1
    base['year_week'] = conn.get_datetime().strftime("%Y-%U")
    base['source'] = base['source'].apply(lambda row: str(row)[:50])

    #delete repeat names
    base['last_name'] = base['last_name'].apply(lambda row: ' '.join(unique_name(row.split())))
    base['full_name'] = base['full_name'].apply(lambda row: ' '.join(unique_name(row.split())))
    
    return base.copy()

def add_sources_suppliers(base: pd.DataFrame):
    try:
        campañas = conn.get_campaigns()
    except:
        campañas = pd.read_pickle('./resources/catalogos/campaigns.pkl')
        campañas = campañas.drop_duplicates('SOURCE').set_index('SOURCE').copy()

    base['SOURCE'] = base['source'].apply(lambda row: row.upper().strip())
    base['supplier'] = base['SOURCE'].map(campañas['supplier'])
    base['origin_campaign'] = base['SOURCE'].map(campañas['campaign_type'])
        
    del base['SOURCE']
    sources_invalids = base['source'][pd.isna(base['supplier'])]
        
    if len(sources_invalids) > 0:
        msg = 'No se encontraron en los catalogos los siguientes origenes: \n'
        for program in sources_invalids.unique():
            msg = msg + '\n\t' + program
    
        #Send warning
        print(msg)

    return base.copy()


def validate_email(base: pd.DataFrame):
    email_black_list = conn.get_email_black_list()
    mx_dns_host = conn.get_dns_host()
    
    base['email_corrected'] = base['email'].apply(lambda row: v_sintax.format_email(row))
       
    base = v_email.validate(base, mx_dns_host)
    
    #Validamos si esta en la black list
    base['email_black_list'] = base['email_corrected'].apply(lambda row: True if row in email_black_list else False)

    print('Se encontraron: ' + str( len( base[base['valid_email'] == False] ) ) + ' email invalidos.')

    return base.copy()


def validate_phone(base: pd.DataFrame):
    phone_black_list = conn.get_phone_black_list()
    
    #clean phone's    
    base['country_code'] = base['country_code'].apply(lambda x: v_sintax.format_phone(str(x)))
    base.loc[base.country_code == '', 'country_code'] =  base.loc[base.country_code == '', 'phone'].apply(lambda x: v_sintax.get_country_code(str(x)))
    
    base['phone_corrected'] = base['phone'].apply(lambda x: v_sintax.format_phone(str(x)))
    
    base = v_phone.validate(base)
    
    #Validamos si esta en la black list
    base['phone_black_list'] = base['phone_corrected'].apply(lambda row: True if row in phone_black_list else False)

    print('Se encontraron: '+ str( len( base[base['valid_phone'] == False] ) ) + ' telefonos invalidos.')

    return base.copy()


def validate_program(base: pd.DataFrame):
    base, programs_invalids = v_program.validate(base)

    if len(programs_invalids) > 0:
        msg = 'No se encontraron en los catalogos los siguientes programas: \n'
        for program in programs_invalids.iterrows():
            program_name = program[1]['corrected_program_name']
            msg = f"{msg} \n\t {program_name} - {program[1]['source']}"

        #Send warning
        requests.post(url_teams, json={'text': msg})
    
    return base.copy()


def validate_campus(base: pd.DataFrame):
    base = v_campus.validate(base)


    return base.copy()



def validate_lead(base: pd.DataFrame):
    base['test'] = base.apply(lambda row: validate_is_test(row['email'], row['full_name']), axis=1)
    
    base['valid_lead'] = base.apply(lambda row: valid_lead(row['valid_email'], 
                                                    row['valid_phone'], 
                                                    row['valid_program'], 
                                                    row['valid_campus'], 
                                                    row['test'],
                                                    row['email_black_list'],
                                                    row['phone_black_list']
                                                ), axis=1)

    return base.copy()


def validate_duplicates(base: pd.DataFrame):
    #Validate duplicates in base
    base, sf_leads = v_duplicates.validate(base)
    
    return base.copy(), sf_leads.copy()


def add_status(status, duplicated, duplicated_sf, valid_lead):
    if valid_lead == False:
        return "Invalido"
    
    if duplicated:
        return "Duplicado en Carga"

    if duplicated_sf:
        return "Duplicado en SF"

    return status



def validate(base: pd.DataFrame):
    #format
    print(f'Se van a procesar {len(base)} datos...')
    print("\n.........")

    base = format_data(base)
    base = add_sources_suppliers(base)

    #add validations
    base = validate_email(base)
    base = validate_phone(base)
    base = validate_program(base)
    base = validate_campus(base)
    
    
    #validate lead
    base = validate_lead(base)
    
    
    #validate duplicates
    base, sf_leads = validate_duplicates(base)


    #add status
    base['status'] = base.apply(lambda row: add_status(row['status'], 
                                                    row['duplicated'], 
                                                    row['duplicated_sf'], 
                                                    row['valid_lead']
                                                    ), axis=1)
    
    
    return base.copy(), sf_leads.copy()
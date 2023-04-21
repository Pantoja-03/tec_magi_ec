import pandas as pd

def add_contact_preference(contact_preference, loaded_phone, email):
    contact_preference = contact_preference.lower()
    
    if len(loaded_phone) < 4:
        return 'Correo electrónico'
    
    if len(email) < 4:
        return 'Teléfono'
    
    if contact_preference == 'phone' or contact_preference == 'telefono' or contact_preference == 'teléfono':
        return 'Teléfono'
    
    if contact_preference == 'whatsapp':
        return 'Whatsapp'
    
    return 'Correo electrónico'


def get_base(base: pd.DataFrame):
    base = base.fillna('')

    #transform data
    base_to_sf = base[[
        'id',
        'name',
        'last_name',
        'status',
        'contact_preference',
        'email',
        'loaded_phone',
        'VEC_Programa__c',
        'modality',
        'VEC_CampusSede__c',
        'owner_id',
        'description',
        'source'
    ]][base['status'] == 'Contacto Nuevo'].copy()

    #add contact preference
    base_to_sf['contact_preference'] = base_to_sf.apply(lambda row: add_contact_preference(row['contact_preference'], row['loaded_phone'], row['email']), axis=1)

    #add other cols
    base_to_sf['RecordTypeId'] = '0122M000001cvioQAA'
    base_to_sf['Company'] = 'VEC'
    base_to_sf['VEC_Origen_Prospecto__c'] = 'Marketing Digital'
    base_to_sf['Campana_Origen__c'] = '7013f0000004pGcAAI'
    base_to_sf['MobilePhone'] = ''
    base_to_sf['Phone'] = ''
    
    base_to_sf['MobilePhone'] = base_to_sf.apply(lambda row: row['loaded_phone'] if row['contact_preference'] == "Whatsapp" else '',axis=1)
    base_to_sf['Phone'] = base_to_sf.apply(lambda row: row['loaded_phone'] if row['contact_preference'] != "Whatsapp" else '',axis=1)
    
    #rename cols
    base_to_sf.rename(columns={
    'id':'Id', 
    'name':'FirstName',
    'last_name':'LastName',
    'status':'Status',
    'contact_preference':'VEC_Preferencia_Contacto__c',
    'email':'Email',
    'modality':'VEC_Modalidad__c',
    'owner_id':'OwnerId',
    'description':'Description',
    'source':'VEC_id_Marketing__c'
    },inplace = True)
    
    
    #format to sf
    base_to_sf = base_to_sf[[
        'Id',
        'RecordTypeId',
        'FirstName',
        'LastName',
        'Status',
        'VEC_Preferencia_Contacto__c',
        'Email',
        'Phone',
        'MobilePhone',
        'VEC_Programa__c',
        'VEC_Modalidad__c',
        'VEC_CampusSede__c',
        'Company',
        'VEC_Origen_Prospecto__c',
        'Campana_Origen__c',
        'OwnerId',
        'Description',
        'VEC_id_Marketing__c',
        ]]

    return base_to_sf.copy()










import pandas as pd

from magi_core import magi_connections as conn

months = {
    1: "Enero",
    2: "Febrero",
    3: "Marzo",
    4: "Abril",
    5: "Mayo",
    6: "Junio",
    7: "Julio",
    8: "Agosto",
    9: "Septiembre",
    10: "Octubre",
    11: "Noviembre",
    12: "Diciembre",
}

days = {
    0: "Domingo",
    1: "Lunes",
    2: "Martes",
    3: "Miércoles",
    4: "Jueves",
    5: "Viernes",
    6: "Sábado",
}


def get_message_document(firstName, lastName, program ,campus, original_date, email, phone):
    #get phone format
    if len(phone) > 10:
        phone = (phone[:-10] + ' ' + phone[-10:-7] + ' ' + phone[-7:-4] + ' ' + phone[-4:]).strip()
   
    #get date format
    day = days.get(int(original_date.strftime('%w')))
    month = months.get(original_date.month)    
    original_date = '{}, {} de {} del {} a las {}:{}'.format(day, original_date.day, month, original_date.year,original_date.hour,original_date.minute)

    #get msg
    msg_sf = open('resources/msg_sf.txt', encoding='utf-8')
    
    msg_sf = msg_sf.read().format(firstName, lastName, program ,campus, original_date, email, phone)

    return msg_sf


def get_base_document(base_duplicates: pd.DataFrame):
    #format to document
    base_duplicates['WhoId'] = base_duplicates['sf_id'] 
    base_duplicates['Description'] = ''
    base_duplicates['RecordTypeId'] = '01241000000yvXXAAY'
    base_duplicates['Status'] = 'Completado'
    base_duplicates['Subject'] = 'Solicitud de información'
    base_duplicates['TaskSubtype'] = 'Task'
    
    base_duplicates['Description'] = base_duplicates.apply(lambda row: get_message_document(
                                                            row['name'],
                                                            row['last_name'],
                                                            row['loaded_program'],
                                                            row['campus'],
                                                            row['original_date'],
                                                            row['email'],
                                                            row['loaded_phone']
                                                        ), axis=1)
    
    base_duplicates = base_duplicates[[
        'WhoId',
        'Description',
        'RecordTypeId',
        'Status',
        'Subject',
        'TaskSubtype'
    ]].to_dict('records')

    return base_duplicates


def get_base_SI(base_sesiones: pd.DataFrame):
    base_sesiones['WhoId'] = base_sesiones['sf_id'] 
    base_sesiones['Description'] = "Este lead dejo sus datos en la convocatoria: \n" + base_sesiones['conference']
    base_sesiones['RecordTypeId'] = '01241000000yvXXAAY'
    base_sesiones['Status'] = 'Completado'
    base_sesiones['Subject'] = 'Activacion'
    base_sesiones['TaskSubtype'] = 'Task'
    
    base_sesiones = base_sesiones[[
        'WhoId',
        'Description',
        'RecordTypeId',
        'Status',
        'Subject',
        'TaskSubtype'
    ]].to_dict('records')
    
    return base_sesiones


def document(base: pd.DataFrame):
    print('\n.........\n')
    base = base.fillna('')
    
    #get base to document
    base_duplicates = base[(base['duplicated_sf'] == True) & 
                            (base['valid_lead'] == True) & 
                            (base['duplicated'] == False)].copy()
    
    
    
    
    #get base sesiones informativas
    origenes_sesiones = [
        'TecdeMtyAcademiaRedContactoPostEvento',
        'TecdeMtyECBDcrmsfMailEvento',
        'TecdeMtyU4PRedContactoPostEvento',
        'TecdeMtymydvisitaMailEvento',
        'TecdeMtyU4PLatamRedContactoPostEvento'
        ]
    
    base_sesiones = base[(base['source'].isin(origenes_sesiones)) & 
                         (base['valid_lead'] == True) & 
                         (base['duplicated'] == False) & 
                         (base['conference'] != '')
                         ].copy()
    
    
    
    #get base exatecs
    origenes_exatec = ['TecdeMtyExatecPortalPrograma']
    base_exatec = base[(base['source'].isin(origenes_exatec)) & 
                         (base['status'] == 'Contacto Nuevo')].copy()
    
    
    #document to sf
    if len(base_duplicates) > 0:
        print('Document duplicates in sf...')
        print('\nDuplicate records: ' + str(len(base_duplicates)))
        
        base_duplicates = get_base_document(base_duplicates)
        
        #one to one
        sf = conn.retry_conect_sf()
        for duplicate in base_duplicates:
            try:
                sf.Task.create(duplicate)
            except:
                try:
                    duplicate['WhatId'] = duplicate.pop('WhoId')
                    sf.Task.create(duplicate)
                except:
                    print("No se pudieron cargar todas las actividades")
        
        # #get connection and document
        # sf = conn.get_conn_sf()
        # res = sf.bulk.Task.insert(base_duplicates)
        
        print('Documented activities')
    
    
    if len(base_sesiones) > 0:
        print('Document Leads from sesiones informativas...')
        print('\nLeads from sesiones informativas: ' + str(len(base_sesiones)))
        
        base_sesiones = get_base_SI(base_sesiones)
        
        #one to one
        sf = conn.retry_conect_sf()
        for sesion in base_sesiones:
            try:
                sf.Task.create(sesion)
            except:
                try:
                    sesion['WhatId'] = sesion.pop('WhoId')
                    sf.Task.create(sesion)
                except:
                    print("No se pudieorn crear las actividades de la activaciones")

        
        print('Documented activations')
    
  
    if len(base_exatec) > 0:
        base_exatec['CampaignId'] = '7018X000001mbITQAY'
        base_exatec['LeadId'] = base_exatec['sf_id'].copy()
        
        base_exatec = base_exatec[['LeadId', 'CampaignId']][~ pd.isna(base_exatec['LeadId'])].to_dict('records').copy()
        
        #one to one
        sf = conn.retry_conect_sf()
        for exatec in base_exatec:
            try:
                sf.CampaignMember.create(exatec)
            except:
                False
  
    print('.........\n')

# -*- coding: utf-8 -*-
"""
Created on Thu Mar 23 21:35:11 2023

@author: segur
"""

import pandas as pd
from datetime import datetime, timedelta
from magi_core import magi_connections as conn
import time
from tqdm import tqdm


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


def document_task(lead, sf):
    for i in range(2):
        time.sleep(1)
        try:
            return sf.Task.create(lead)
            break
        
        except:
            pass
    
    return None


def get_message_document(original_date, activity_date):
   
    #get date format
    day = days.get(int(original_date.strftime('%w')))
    month = months.get(original_date.month)    
    hora = original_date.hour
    minutos = original_date.minute
    
    if hora < 10:
        hora = f'0{hora}'
        
    if minutos < 10:
        minutos = f'0{minutos}'
    
    original_date = '{}, {} de {} del {} a las {}:{}'.format(day, original_date.day, month, original_date.year,hora, minutos)
    
    
    day = days.get(int(activity_date.strftime('%w')))
    month = months.get(activity_date.month)    
    hora = activity_date.hour
    minutos = activity_date.minute
    
    if hora < 10:
        hora = f'0{hora}'
        
    if minutos < 10:
        minutos = f'0{minutos}'

    activity_date = '{}, {} de {} del {} a las {}:{}'.format(day, activity_date.day, month, activity_date.year,hora,minutos)
        

    msg_sf = f"Este lead pidio informes el {original_date} y espera ser contactado antes del {activity_date}"
    return msg_sf




def get_base_document(new_base: pd.DataFrame):
    minutos = 10
    activity_date = datetime.now() + timedelta(minutes = minutos)
    
    if ((activity_date.hour == 19) & (activity_date.minute > 50)) or ((activity_date.hour >= 20) or (activity_date.hour < 9)):
        if (activity_date.hour >= 20):
                activity_date = activity_date + timedelta(days = 1)
        
        activity_date = datetime(activity_date.year, activity_date.month, activity_date.day, 9,0,0)


    while activity_date.isoweekday() > 5:
        activity_date = activity_date + timedelta(days = 1)
    
    
    
    new_base['WhoId'] = new_base['sf_id'] 
    new_base['Priority'] = 'Alta'
    new_base['RecordTypeId'] = '01241000000yvXXAAY'
    
    new_base['Subject'] = new_base['origin_campaign'].apply(lambda x: 'Seguimiento lead organico' if x == 'Organico' else 'Seguimiento lead inorganico')
    
    #new_base['Subject'] = 'Lead en espera de ser contactado'
    new_base['TaskSubtype'] = 'Task'
    new_base['ActivityDate'] = str(activity_date.date())
    new_base['OwnerId'] = new_base['owner_id']
    #new_base['ReminderDateTime'] = str(activity_date)[:10] + "T" + str(activity_date)[11:19] + "-06:00"
    
    
    new_base['Description'] = new_base['created_at'].apply(lambda row: get_message_document(row,activity_date) ) 
    
    new_base = new_base[[
        'WhoId',
        'Description',
        'RecordTypeId',
        'Subject',
        'TaskSubtype',
        'ActivityDate',
        'OwnerId',
        'Priority',
    #    'ReminderDateTime'
    ]].to_dict('records')
    
    return new_base.copy()




def document(base: pd.DataFrame):
    print('\n.........\n')
    base = base.fillna('')
    
    new_base = base[base['status'] == 'Contacto Nuevo'].copy()
    
    #document to sf
    if len(new_base) > 0:
        print('Creando tareas a leads nuevos...')
        print('\nLeads nuevos: ' + str(len(new_base)))
        
        new_base = get_base_document(new_base)
        
        #one to one
        sf = conn.retry_conect_sf()
        for lead in tqdm(new_base):
            try:
                document_task(sf, lead)
            except:
                pass
        
        print('Nuevos leads documentados...')
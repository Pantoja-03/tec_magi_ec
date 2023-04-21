# -*- coding: utf-8 -*-
"""
Created on Tue Jun  9 16:46:29 2020

@author: Andrés Pantoja

Magi - Validate Sintax
"""
from unidecode import unidecode
import re

DMNS = [
    '.es'
    '.comk',
    '.coml',
    ',com',
    '.comm',
    '.com.de',
    '.clm',
    '.com.com',
    '.vom',
    '.con',
    '.icom',
    '.comotmail',
    '.como',
    '.comcom',
    '.coma',
    '.co',
    '.cim',
    '.xom',
]

DMN_GML = [
    '@gmil',
    '@gmal',
    '@gmai',
    '@gmial',
    '@gmmail',
    '@gmaill',
    '@gmaiil',
    '@gmaii',
]

DMN_OTK = [
    '@outllook',
    '@oytlook',
    '@otlook',
    '@outlok',
    '@outook',
    '@oulook',
    '@outloo',
    '@outloock',
    '@outloook',

]

DMN_HML = [ 
    '@htomail', 
    '@hoymail', 
    '@hltmail', 
    '@hotmail', 
    '@hotmaail',
    '@hormail', 
    '@hotamail',  
    '@hotmal',
    '@hotmall',
    '@hotnail',
    '@hotmial',
    '@hotmaul',
    '@hotmall',
    '@hotmaio',
    '@hotmaill',
    '@hotmai',
    '@hotma',   
    '@homail',
    '@holmail',
    '@hmail',     
]

def format_email(email):
    email = unidecode(email)
    email = email.replace("'","")
    email = email.replace('"','')
    email = email.replace(' ','')
    email = email.strip()
    email = email.lower()

    #Separamos el correo del dominio y de su extension
    ema = email[:email.find("@")]
    dom = email[email.find("@"):]
    ext = dom[dom.find("."):]
    dom = dom[:dom.find(".")]


    if dom in DMN_GML:
        email = ema + "@gmail.com"

    elif dom in DMN_HML:
        if ext == ".es":
            email = ema + "@hotmail.es"
        else:
            email = ema + "@hotmail.com"

    elif dom in DMN_OTK:
        if ext == ".es":
            email = ema + "@outlook.es"
        else:
            email = ema + "@outlook.com"
    elif ext in DMNS:
        email = ema + dom + ".com"
        

    #Reglas adicionales
    email = email.replace('.com.com','.com')
    email = email.replace('-com','.com')
    email = email.replace('outlook.com.mx','outlook.com')
    email = email.replace('hotmail.com.mx','hotmail.com')
    email = email.replace('gmail.com.mx','gmail.com')
    email = email.replace('gmail.edu.mx','gmail.com')
    email = email.replace('live.com.mx','live.com')
    

    return email 
    

def format_phone(phone):
    phone = re.sub('[a-zA-Z]', '', phone)
    phone = phone.replace('.','')
    phone = phone.replace('-','')
    phone = phone.replace('_','')
    phone = phone.replace(' ','')
    phone = phone.replace('+','')
    phone = phone.replace("'","")
    phone = phone.replace('(','')
    phone = phone.replace(')','')
    phone = phone.replace('´','')
    
    phone = phone.strip()
    
    if phone[:2] == '00':
        phone = phone[2:]
    if phone[:4] == '5201':
        phone = phone[4:]
    if phone[:3] == '521':
        phone = phone[3:]
    if phone[:2] == '52':
        phone = phone[2:]
        
        
    phone = phone[-15:]


    return phone

def get_country_code(phone):
    phone = re.sub('[a-zA-Z]', '', phone)
    phone = phone.replace('.','')
    phone = phone.replace('-','')
    phone = phone.replace('_','')
    phone = phone.replace(' ','')
    phone = phone.replace('+','')
    phone = phone.replace("'","")
    phone = phone.replace('(','')
    phone = phone.replace(')','')
    phone = phone.replace('´','')
    
    phone = phone.strip()
    
    if phone[:2] == '00':
        phone = phone[2:]
    if phone[:3] == '521':
        phone = phone[3:]
    if phone[:2] == '52':
        phone = phone[:2]
        
    if len(phone) > 2:
        phone =  ''

    return phone

def format_name(name):    
    name = unidecode(name)
    name = name.replace('Ã','')
    name = name.replace('°','')
    name = name.replace('','')
    name = name.replace('  ',' ')
    name = name.replace(',','')
    name = name.replace('"','')
    name = name.replace('<','')
    name = name.replace('>','')
    name = name.replace('$','')
    name = name.replace('%','')
    name = name.replace('(','')
    name = name.replace(')','')
    name = name.replace('[','')
    name = name.replace(']','')
    name = name.replace('!','')
    name = name.replace('¡','')
    name = name.replace('.','')
    name = name.replace('-','')
    name = name.replace('_',' ')
    name = name.replace('+','')
    name = name.replace('@','')
    name = name.replace('?','')
    name = name.replace('/','')
    name = name.replace('=','')
    name = name.replace('|','')
    name = name.replace('\n','')
    name = name.replace('\t','')
    name = name.replace('0','')
    name = name.replace('1','')
    name = name.replace('2','')
    name = name.replace('3','')
    name = name.replace('4','')
    name = name.replace('5','')
    name = name.replace('6','')
    name = name.replace('7','')
    name = name.replace('8','')
    name = name.replace('9','')
    name = name.replace("'","")
    name = name.replace('  ',' ')
    name = name.replace('n/d', '')
    name = name.replace('null','')
    name = name.replace('Null','')
    name = name.replace('N/D', '')
    name = name.replace('NULL','')
   
    name = name.strip()
    if name == 'nan' or name == "" or name == "X":
        name =  ''

    return name.title()

def format_comments(text):
    text = unidecode(text)
    text = text.replace('Ã','')
    text = text.replace('°','')
    text = text.replace('','')
    text = text.replace('  ',' ')
    text = text.replace('\n','')
    text = text.replace('\t','')
    text = text.replace('  ',' ')
    text = text.replace('N/D', '')
    text = text.replace('NULL','')
    
    if text == 'nan' or text == '':
        text =  ' '
    return text


def get_lastName(lastName1, lastName2):
    if lastName1 != 'nan' and lastName2 != 'nan' and len(lastName1) > 0 and len(lastName2) > 0:
        lastName = str(lastName1) +" "+ str(lastName2)
    elif lastName1 == 'nan' or len(lastName1) < 1 :
        lastName = 'X'
    else:
        lastName = lastName1
    
    return lastName

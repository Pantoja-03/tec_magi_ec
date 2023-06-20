import pandas as pd
import gsuite_api.gsuite as gsuite
from simple_salesforce import Salesforce
from sqlalchemy import create_engine
import MySQLdb as mysql
from datetime import datetime
import os
from dotenv import load_dotenv
import pytz
from unidecode import unidecode
import time
import numpy as np
import yaml
from pathlib import Path
import requests
import smtplib
from glob import glob

load_dotenv()


def get_conn_sf():
    return Salesforce(
        username = os.getenv('SF_USERNAME'),
        password = os.getenv('SF_PASSWORD'),
        security_token = os.getenv('SF_TOKEN')
        )


def retry_conect_sf():
    for i in range(4):
        time.sleep(1)
        try:
            return get_conn_sf()
            break
        except:
            pass
    
    raise Exception("Error al conectarse con SF")
    


def get_url_teams():
    return os.getenv('URL_TEAMS')


def read_soql_sf(soql):
    sf = get_conn_sf()
    
    res = pd.DataFrame(sf.query_all(soql)['records'])
    
    return res


def get_conn_db():

    conn = mysql.connect(
            host = os.getenv('DB_HOST'),
            user = os.getenv('DB_USERNAME'),
            passwd = os.getenv('DB_PASSWORD'),
            db = os.getenv('DB_DATABASE'), 
            connect_timeout = 40
            )
       
    return conn


def get_engine_db():
    
    db_drive = os.getenv('DB_CONNECTION')
    db_user = os.getenv('DB_USERNAME')
    db_pass = os.getenv('DB_PASSWORD')
    db_host = os.getenv('DB_HOST')
    db_database = os.getenv('DB_DATABASE')
    
    
    engine = create_engine(f"{db_drive}://{db_user}:{db_pass}@{db_host}/{db_database}", connect_args={'connect_timeout' : 30})
                               
    return engine


def get_datetime():
    now = datetime.now(pytz.timezone('America/Chihuahua'))

    return pd.to_datetime(now.strftime("%Y-%m-%d %H:%M:%S"))


def read_sql(query):
    engine = get_engine_db()
        
    res = pd.read_sql(query, engine)
    
    return res.copy()



def get_mailer():
    s = smtplib.SMTP(host = os.getenv('MAIL_HOST'), port = os.getenv('MAIL_PORT'))
    
    s.starttls()
    
    s.login(os.getenv('MAIL_USERNAME'), os.getenv('MAIL_PASSWORD'))
    
    return s 



def get_base_assignments():

    query_assignments = """
        SELECT * 
        FROM assignments
    """
    
    base_assignments = read_sql(query_assignments)
    base_assignments['key'] = base_assignments['owner'].astype(str) + base_assignments['date'].astype(str)
    
    return base_assignments.copy()


def read_yaml(file_path: str):
    if not file_path.endswith('.yml'):
        file_path += '.yml'
    f = open(file_path, 'r', encoding='utf-8')
    res = yaml.load(f.read(), Loader=yaml.FullLoader)
    f.close()
    return res


def to_yaml(obj, file_path, snapshot=True):
    if not file_path.endswith('.yml'):
        file_path += '.yml'
    path = Path(file_path)

    f = path.open('w', encoding='utf-8')
    yaml.dump(obj, f, encoding='utf-8', allow_unicode=True)
    f.close()


def get_resume_leads(start_date = '2021-07-01', end_date = None):
  if end_date is None:
    end_date = str(end_date)
  
  engine = get_engine_db()
  
  query = f"""
      SELECT
          assignment_date as fecha,
          year_week as semana,
          owner_region as region,
          owner_name as asesor
      FROM leads
      WHERE status = 'Cargado' and 
          assignment_type in ('Asignacion por carrusel', 'Asignacion por carrusel x2', 'Asignacion por carrusel x3', 'Asignacion por carrusel - offline', 'Asignacion por carrusel - offline x2', 'Asignacion por carrusel - offline x3') 
          and owner_name <> '' 
          and created_at > '{start_date}' 
          and created_at <= '{end_date}'
      """
      
  engine.dispose()
  
  return pd.read_sql(query, engine)


def get_cubo_ec(): 
    return pd.read_csv(
              r'G:\.shortcut-targets-by-id\11jNJkPFSMwK6AvDuAdiz0e0pfqjRD2fl\01 Procesos\01 Cubo VEC\02 U4P - EC\base.csv',
              usecols=[
                'Rol del propietario',
                'Usuario Activo',
                'Estatus Integrado',
                'Periodo',
                'Sistema origen',
                'Propietario',
                'Monto',
                'Region del asesor',
                'Fecha de creacion',
                'Antiguedad',
                  ],
              dtype={
                  'Usuario Activo': np.float64,
                  'Monto': np.float64,
                  'Rol del propietario': str,
                  'Region del asesor': str
                  }
            )



def update_processing_leads(leads_id : list):
    try:
        query = f"""
            UPDATE leads 
            SET 
                status = 'procesando'
            WHERE id in {leads_id}
        """.replace('[','(').replace(']',')')

        conn = get_conn_db()

        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()

        return True
        
    except:
        return False
    
    finally:
       cursor.close()
       conn.close()


def get_iup_leads(status = 'created', pendientes = False):
    
    try:
        leads_iup = """
            SELECT * 
            FROM leads
           
            WHERE status = '{}'
        """.format(status)
        
        base = read_sql(leads_iup)
            
        base['processing_date'] = get_datetime()
            
        if pendientes == False:
            update_processing_leads(list(base['id'].unique()))
    
        return base.copy()

    except Exception as e:
        raise Exception(e)


def get_programs_drive():
    res = gsuite.get_sheet_data("1KS0aC7HmemmLPqkqzFegQZufMT2_41hyyIHcCVKUtcI", "Programas Activos!A:C")
    
    res['Id Salesforce'] = res['Id Salesforce'].apply(lambda x: x[:15])

    return res.copy()


def get_campus_drive():
    res = gsuite.get_sheet_data("1KS0aC7HmemmLPqkqzFegQZufMT2_41hyyIHcCVKUtcI", "Campus Activos!A:D")
    return res.copy()


def get_specials_programs():
    special = gsuite.get_sheet_data("1KS0aC7HmemmLPqkqzFegQZufMT2_41hyyIHcCVKUtcI", "Programas Especiales!A:A")
    special = list(special['Programa'].str.upper().str.strip())
    return special.copy()


def get_strategic_programs():
    strategic = gsuite.get_sheet_data("1KS0aC7HmemmLPqkqzFegQZufMT2_41hyyIHcCVKUtcI", "Programas Estrategicos!A:A")
    strategic = list(strategic['Programa'].str.upper().str.strip())
    return strategic.copy()


def get_assignment_programs():
    assignment = gsuite.get_sheet_data("1KS0aC7HmemmLPqkqzFegQZufMT2_41hyyIHcCVKUtcI", "Programas Asignacion!A:A")
    assignment = list(assignment['Programa'].str.upper().str.strip())
    return assignment.copy()


def get_assignment_direct_programs():
    assignment = gsuite.get_sheet_data("1KS0aC7HmemmLPqkqzFegQZufMT2_41hyyIHcCVKUtcI", "Programas Asignacion Directa!A:A")
    assignment = list(assignment['Programa'].str.upper().str.strip())
    return assignment.copy()

def get_program_invalids():
    res = gsuite.get_sheet_data("1KS0aC7HmemmLPqkqzFegQZufMT2_41hyyIHcCVKUtcI", "Programas Invalidos!A:B")
    res = res.fillna("")
    res['Programa'] = res['Programa'].apply(lambda x: x.title().replace('  ',' ').replace('—','-').strip())

    res.to_pickle('./resources/catalogos/program_invalids.pkl')

    return res.copy()

def get_email_black_list():
    #res = pd.read_excel(r'..\..\03 Carga Leads\00 resources\black_list.xlsx')
    res = gsuite.get_sheet_data("1iupk_dzUV63xklOK1TWppA6CqbElUpvZJhiiYrHoS4M", "Black list!A:C")
    res = res[~ pd.isna(res['Black list'])]
    res['Tipo'] = res['Tipo'].apply(lambda x: str(x).capitalize().strip()) 
    
    res = res[res['Tipo'] == 'Correo']
    
    return list(res['Black list'].str.lower().unique())

def get_phone_black_list():
    #res = pd.read_excel(r'..\..\03 Carga Leads\00 resources\black_list.xlsx')
    res = gsuite.get_sheet_data("1iupk_dzUV63xklOK1TWppA6CqbElUpvZJhiiYrHoS4M", "Black list!A:C")
    res = res[~ pd.isna(res['Black list'])]
    
    res['Tipo'] = res['Tipo'].apply(lambda x: str(x).capitalize().strip()) 
    
    res = res[res['Tipo'] == 'Telefono']
    
    return list(res['Black list'].str.lower().unique())


def get_special_rules():
    res = gsuite.get_sheet_data("1KS0aC7HmemmLPqkqzFegQZufMT2_41hyyIHcCVKUtcI", "Reglas de asignacion especiales!A:D")
    res = res[res['active'] == '1']
    
    res = res.drop_duplicates('rule_name', keep='last')
    return res.copy()


def get_rule_filters():
    res = gsuite.get_sheet_data("1KS0aC7HmemmLPqkqzFegQZufMT2_41hyyIHcCVKUtcI", "Filtros reglas!A:E")
    res = res.replace('0', False).replace('1', True)
    
    return res.copy()



def get_programs_sf():
    
    soql_programs = """
        SELECT 
            hed__Account__c,
            hed__Account__r.Parent.Name, 
            hed__Account__r.Type,
            hed__Account__r.VEC_Area_tematica__c,
            VEC_Nombre_largo__c,
            VEC_Modalidad__c,
            hed__Start_Date__c
        FROM hed__Program_Plan__c
        Where RecordTypeId = '0122M000001cviqQAA' And hed__Account__r.Type not in ('Maestría', 'Especialidad', 'Doctorado')
    """
    
    sf = retry_conect_sf()
    res = pd.DataFrame(sf.query_all(soql_programs)['records'])
    
    res['Escuela'] = res['hed__Account__r'].apply(lambda x: x['Parent']['Name'] if not pd.isna(x) else None)
    res['Tipo de programa'] = res['hed__Account__r'].apply(lambda x: x['Type'] if not pd.isna(x) else None)
    res['Area tematica'] = res['hed__Account__r'].apply(lambda x: x['VEC_Area_tematica__c'] if not pd.isna(x) else None)
    
    del res['attributes']
    del res['hed__Account__r']
    
    res.sort_values('hed__Start_Date__c', ascending = False, inplace=True)
    res = res.drop_duplicates('VEC_Nombre_largo__c', keep='first')

    res.rename(columns={
        'VEC_Nombre_largo__c':'Programa Salesforce',
        'hed__Account__c':'Id Salesforce',
        'VEC_Modalidad__c':'Modalidad',
        }, inplace=True)
    
    res['Programa'] = res['Programa Salesforce'].copy()
    
    res.dropna(subset=['Id Salesforce'], inplace=True)
    
    res['Id Salesforce'] = res['Id Salesforce'].apply(lambda x: x[:15])
    
   
    return res[['Programa','Programa Salesforce','Id Salesforce','Area tematica','Tipo de programa', 'Escuela', 'Modalidad']]

    
def get_campus_sf():
    soql_campus = """
        SELECT 
           Name, 
           Id,
           Zona_Regional__c
        FROM Account
        Where RecordTypeId = '0122M000001cvf5QAA' and activo__c = True and (Zona_Regional__c!= '')
    """

    sf = retry_conect_sf()
    res = pd.DataFrame(sf.query_all(soql_campus)['records'])

    #Apply changes
    res['Name'] = res['Name'].apply(lambda row: row.replace('TEC ',''))
    res['Zona_Regional__c'] = res['Zona_Regional__c'].apply(lambda row: row.replace('Región Cd. de México','Región México'))
    res['Zona_Regional__c'] = res.apply(lambda row: "Región EGOB" if (row['Name'].upper().find('EGOB') >= 0) | (row['Name'].upper().find('ESCUELA DE GOBIERNO') >= 0) else row['Zona_Regional__c'], axis=1)
    res['Zona_Regional__c'] = res.apply(lambda row: "Región EGADE" if row['Name'].upper().find('EGADE') >= 0 else row['Zona_Regional__c'], axis=1)
    
    res.rename(columns={
        'Name':'Campus Salesforce',
        'Id':'Id Salesforce',
        'Zona_Regional__c':'Region'
        }, inplace=True)
    
    res['Campus'] = res['Campus Salesforce'].copy()
    res['Id Salesforce'] = res['Id Salesforce'].apply(lambda row: row[:15])
    

    return res[['Campus','Campus Salesforce','Region','Id Salesforce']]

def get_program_catalogs():
    drive = get_programs_drive()
    sf = get_programs_sf()
    
    sf_index = sf[['Id Salesforce', 'Area tematica', 'Tipo de programa', 'Escuela', 'Modalidad']].drop_duplicates('Id Salesforce').set_index('Id Salesforce')
    
    
    drive['Area tematica'] = drive['Id Salesforce'].map(sf_index['Area tematica'])
    drive['Tipo de programa'] = drive['Id Salesforce'].map(sf_index['Tipo de programa'])
    drive['Escuela'] = drive['Id Salesforce'].map(sf_index['Escuela'])
    drive['Modalidad'] = drive['Id Salesforce'].map(sf_index['Modalidad'])
    
    catalog = pd.concat([drive, sf], ignore_index=True)
    
    catalog['ALIAS'] = catalog['Programa'].apply(lambda x: unidecode(x).upper().replace('  ',' ').replace('—','-').strip())
    catalog = catalog.drop_duplicates('ALIAS')

    catalog.to_pickle('./resources/catalogos/program_catalogs.pkl')
    
    return catalog.set_index('ALIAS')


def get_campus_catalogs():
    drive = get_campus_drive()
    sf = get_campus_sf()
    
    catalog = pd.concat([drive, sf], ignore_index=True)
    
    catalog['Campus'] = catalog['Campus'].apply(lambda x: unidecode(x).upper().replace('  ',' ').replace('—','-').strip())
    catalog = catalog.drop_duplicates('Campus')
    
    catalog.to_pickle('./resources/catalogos/campus_catalogs.pkl')
    
    return catalog.set_index('Campus')


def get_campaigns():
    engine = create_engine("mysql://iup_posgrados:2r&h46Rb@18.204.133.156/tecdap_iup_posgrados")
    
    query = "SELECT * FROM campaigns"
    
    res = pd.read_sql(query, engine)
    
    res['SOURCE'] = res['name'].apply(lambda row: row.upper().strip())

    res.to_pickle('./resources/catalogos/campaigns.pkl')
        
    return res.drop_duplicates('SOURCE').set_index('SOURCE').copy()



def get_sf_prospectos(last_days = 100):
    soql_prospectos = f"""
        SELECT 
        ID, 
        Status, 
        Email, 
        Phone,
        MobilePhone,
        VEC_Programa__c,
        OwnerId,
        CreatedDate
        FROM Lead 
        WHERE CreatedDate = LAST_N_DAYS:{last_days} 
            and RecordTypeID = '0122M000001cvioQAA' 
            and Status in ('Contactado', 'Contacto Nuevo') 
            and Email <> ''
            and VEC_Programa__r.Type not in ('Maestría', 'Especialidad', 'Doctorado') 
        ORDER BY CreatedDate desc

    """
 
    res = read_soql_sf(soql_prospectos)
    
    res.rename(columns={'VEC_Programa__c':'Programa'}, inplace=True)
    res['Programa'] = res['Programa'].str[:15]
    res['Etapa'] = "Prospectos"
    
    return res[['Id', 'Status', 'Email','Phone','MobilePhone','Programa', 'Etapa', 'OwnerId', 'CreatedDate']]


def get_sf_opp(last_days = 100):
    soql_opp = f"""
            SELECT
            ID,
            StageName,
            VEC_Correo_electronico_contacto__c,
            VEC_Program_plan__r.hed__Account__c,
            VEC_BusinessContact__r.Phone,
            VEC_BusinessContact__r.Mobilephone,
            OwnerId,
            CreatedDate
            FROM Opportunity
            WHERE recordtypeid = '0122M000001cvipQAA' 
                and CreatedDate = LAST_N_DAYS:{last_days} 
                and StageName in ('Solicitud de Inscripción', 'Seguimiento', 'Oportunidad', 'Solicitud de Inscripción - Incompleto', 'Seguimiento - Contactado') 
                and Subetapa__c <> 'Rechazado'
                and VEC_Correo_electronico_contacto__c <> ''
                and VEC_Program_plan__r.hed__Account__r.Type not in ('Maestría', 'Especialidad', 'Doctorado')
            ORDER BY CreatedDate desc
        """
        
    res = read_soql_sf(soql_opp)
       
    res['Programa'] = res['VEC_Program_plan__r'].apply(lambda x: x['hed__Account__c'] if not pd.isnull(x) else None)
    res['Phone'] = res['VEC_BusinessContact__r'].apply(lambda x: x['Phone'] if not pd.isnull(x) else None)
    res['MobilePhone'] = res['VEC_BusinessContact__r'].apply(lambda x: x['MobilePhone'] if not pd.isnull(x) else None)

    res.rename(columns={'VEC_Correo_electronico_contacto__c':'Email', 'StageName':'Status'}, inplace=True)
    res['Programa'] = res['Programa'].str[:15]
    res['Etapa'] = "Oportunidad"
    

    return res[['Id', 'Status', 'Email','Phone','MobilePhone','Programa', 'Etapa', 'OwnerId', 'CreatedDate']]


def get_sf_leads(last_days = 100):
    sf_leads = pd.concat([get_sf_prospectos(last_days), get_sf_opp(last_days)], ignore_index=True)
    #sf_leads = sf_leads.drop_duplicates('Email')
    
    sf_leads['KEY'] = sf_leads.apply(lambda row: str(row['Email']).upper().strip() + str(row['Programa']).upper().strip(),axis=1)
    sf_leads['duplicated'] = sf_leads.duplicated(subset=['KEY'])
    
    sf_leads.to_pickle('./resources/catalogos/sf_leads.pkl')

    return sf_leads.copy()


def get_dns_host():
    try:
        mx_dns_host = pd.read_pickle('./resources/dns_hosts.pkl')
        mx_dns_host = mx_dns_host.to_dict(orient='dict')[0]
    except:
        mx_dns_host = {
            'gmail.com':True,
            'outlook.com':True,
            'hotmail.com':True,
            'icloud.com':True,
            'live.com':True,
            'yahoo.com':True
            }
    
    return mx_dns_host


    
def get_sf_users():
    users = gsuite.get_sheet_data("1KS0aC7HmemmLPqkqzFegQZufMT2_41hyyIHcCVKUtcI", "Usuarios!A:I")
    
    return users.copy()


def get_users_asignator():
    url = "https://asignator.tecdap.net/api/v1/c_sessions"
    payload={}
    headers = {'Authorization': 'Bearer 643f0ee9c363f005c736e540'}
    response = requests.request("GET", url, headers=headers, data=payload)
    asesores_activos = list(set(response.text.replace('[','').replace('"','').replace(']','').split(',')))
    
    return asesores_activos


def get_users_60m(fecha = get_datetime()):
    query = f"""
        SELECT owner
        From leads
        Where assignment_type = "Asignacion por carrusel - online" and processing_date > DATE_SUB("{fecha}", INTERVAL 31 MINUTE)
    """
    
    res = read_sql(query)
    
    if len(res) > 0:
        res = list(set(res['owner']))
    
        return res
    
    else: 
        return [] 
    

def format_final_number(number):
    while len(number) < 4:
        number = "0" + number

    return number

def get_mx_plan_marcacion():
    path = r"resources/mx_plan_marcacion/"

    mx_plan_marcacion = pd.read_csv(glob(path + "csv/*.csv")[-1], index_col=False)

    mx_plan_marcacion[' NUMERACION_INICIAL'] = mx_plan_marcacion.apply(lambda row: format_final_number(str(row[' NUMERACION_INICIAL'])),axis=1)
    mx_plan_marcacion[' NUMERACION_FINAL'] = mx_plan_marcacion.apply(lambda row: format_final_number(str(row[' NUMERACION_FINAL'])),axis=1)    
    
    mx_plan_marcacion['IX'] = mx_plan_marcacion.apply(lambda row: str(row[' NIR']) + str(row[' SERIE']),axis=1)
    mx_plan_marcacion['START'] = mx_plan_marcacion.apply(lambda row: str(row[' NIR']) + str(row[' SERIE']) + str(row[' NUMERACION_INICIAL']),axis=1)
    mx_plan_marcacion['END'] = mx_plan_marcacion.apply(lambda row: str(row[' NIR']) + str(row[' SERIE']) + str(row[' NUMERACION_FINAL']),axis=1)

    mx_plan_marcacion.rename(columns={' ESTADO':'ESTADO',' MUNICIPIO':'MUNICIPIO',' NIR':'LADA'}, inplace=True)
    
    mx_plan_marcacion = mx_plan_marcacion[['ESTADO','MUNICIPIO','LADA','IX','START', 'END']]


    mx_plan_marcacion.to_csv(path + "pnm.csv", index=False)
    mx_plan_marcacion.to_pickle(path + "pnm.pkl")

    return mx_plan_marcacion.copy()



def get_base_ecommerce():
    soql_reassignament = """
        SELECT
        Id,
        CreatedDate,
        VEC_Program_plan__r.VEC_Nombre_largo__c,
        VEC_Program_plan__r.hed__Start_Date__c,
        VEC_Program_plan__r.hed__Account__r.Parent.Name,
        VEC_BusinessContact__r.Nombre_Completo__c,
        VEC_Correo_electronico_contacto__c,
        VEC_BusinessContact__r.Phone,
        VEC_BusinessContact__r.MobilePhone,
        Campus_Sede__r.Name,
        VEC_OppOrigin__c,
        StageName,
        Subetapa__c,
        OwnerId,
        VEC_Modalidad__c
        FROM Opportunity
        WHERE RecordTypeId = '0122M000001cvipQAA' and OwnerId = '0053f000000Fe2SAAS' 
        and StageName in ('Solicitud de Inscripción')
        """
    
    sf = get_conn_sf()
    res = pd.DataFrame(sf.query_all(soql_reassignament)['records'])
    
    res['name'] = res['VEC_BusinessContact__r'].apply(lambda x: x['Nombre_Completo__c'] if not pd.isnull(x) else None)
    res['program_name'] = res['VEC_Program_plan__r'].apply(lambda x: x['VEC_Nombre_largo__c'] if not pd.isnull(x) else None)
    res['campus'] = res['Campus_Sede__r'].apply(lambda x: x['Name'] if not pd.isnull(x) else None)
    res['start_date'] = res['VEC_Program_plan__r'].apply(lambda x: x['hed__Start_Date__c'] if not pd.isnull(x) else None)
    res['school'] = res['VEC_Program_plan__r'].apply(lambda x: x['hed__Account__r'] if not pd.isnull(x) else None)
    res['school'] = res['school'].apply(lambda x: x['Parent']['Name'] if not pd.isnull(x) else None)
    
    res['phone'] = res['VEC_BusinessContact__r'].apply(lambda x: x['Phone'] if not pd.isnull(x) else None)
    res['mobilephone'] = res['VEC_BusinessContact__r'].apply(lambda x: x['MobilePhone'] if not pd.isnull(x) else None)
    
    res.loc[pd.isna(res.phone), 'phone'] = res.loc[pd.isna(res.phone), 'mobilephone']
   
    res.rename(
        columns={
            'Id' : 'id',
            'CreatedDate' : 'sf_creation_date',
            'VEC_Correo_electronico_contacto__c':'email', 
            'StageName':'stage', 
            'Subetapa__c':'sub_stage',
            'VEC_OppOrigin__c':'source',
            'VEC_Modalidad__c':'modality'}, 
            inplace=True)

    res['processing_date'] = get_datetime()
    res['last_name1'] = ''
    res['last_name2'] = ''
    res['description'] = ''
    res['program_type'] = ''
    res['country_code'] = ''

    return res[['id','sf_creation_date', 'processing_date', 'start_date','name', 'last_name1', 'last_name2', 'email', 'country_code', 'phone','program_name', 'program_type', 'campus', 'school', 'description', 'source', 'stage', 'sub_stage', 'modality']].copy()
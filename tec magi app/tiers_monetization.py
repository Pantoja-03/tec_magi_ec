# -*- coding: utf-8 -*-
"""
Created on Mon Apr 24 09:44:26 2023

@author: segur
"""

import pandas as pd
import gsuite_api.gsuite as gsuite
import gspread
from df2gspread import df2gspread as d2g
from datetime import date, timedelta
from magi_core import magi_connections as magi_conn
from sqlalchemy import create_engine



def cuadrante_actual(x_limit, y_limit, x, y):
  if x > x_limit:
    if y > y_limit:
      return 3
    else:
      return 1
  else:
    if y > y_limit:
      return 4
    else:
      return 2


#GET LEADS
leads = magi_conn.get_resume_leads(
    start_date=date.today() - timedelta(days=100),
    end_date=date.today() - timedelta(days=1))

#leads_asignados = leads.groupby('asesor')['leads'].sum()
leads_asignados = leads['asesor'].value_counts()



#GET CUBO
cubo = magi_conn.get_cubo_ec()
cubo['Fecha de creacion'] = pd.to_datetime(cubo['Fecha de creacion'], yearfirst=True)
cubo['Antiguedad'].value_counts()

cubo.loc[cubo.Propietario == 'Angélica Mayra Peralta Flores', 'Usuario Activo'] = 0

cubo = cubo[
      (cubo['Rol del propietario'] == 'VEC Asesor U4P') &
      (cubo['Usuario Activo'] == 1.0) &
      (cubo['Estatus Integrado'] == '07 Inscrito') &
      # (cubo['Periodo'].isin(['JD20' ,'EJ21'])) &
      (cubo['Antiguedad'].isin(['01 Nueva' ,'02 Madura'])) &
      (cubo['Sistema origen'] == 'SF')
    ]



ventas = cubo.groupby(['Propietario'])['Monto'].sum().astype(int)
regiones = cubo.dropna()[['Propietario', 'Region del asesor']].drop_duplicates('Propietario').set_index('Propietario')['Region del asesor'].to_dict()



#GET ASESORES
asesores = pd.DataFrame({
        'leads': leads_asignados,
        'ventas': ventas
    }).reset_index()


asesores['monetizacion'] = asesores['ventas'].fillna(0) / asesores['leads']
asesores['region'] = asesores['index'].map(regiones)

asesores = asesores[
      (~pd.isna(asesores['region'])) & 
      (asesores['region'] != 'Región en Línea') &
      #(asesores['region'] != 'Región Internacional') &
      (asesores['region'] != 'Región TLG') &
      (asesores['region'] != 'Región Programas Estrategicos') &
      (asesores['index'] != 'Karen Duarte')
    ].copy()


x = round(asesores[(asesores['monetizacion'] > 0) & (asesores['region'] != 'Región Internacional')]['monetizacion'].mean())
y = round(asesores[(asesores['monetizacion'] > 0) & (asesores['region'] != 'Región Internacional')]['leads'].mean())


asesores['cuadrante_actual'] = asesores.apply(
    lambda row: f"Cuadrante {cuadrante_actual(x, y, row['monetizacion'], row['leads'])}", axis=1
  )

asesores['cuadrante_actual'].value_counts()


asesores['tier'] = 1
asesores.loc[asesores['cuadrante_actual'].isin(['Cuadrante 3', 'Cuadrante 1']), 'tier'] = 2
asesores.groupby('tier')['region'].count()


limite_3 = asesores[asesores['monetizacion'] < 5000]['monetizacion'].quantile(0.90)
asesores.loc[asesores['cuadrante_actual'].isin(['Cuadrante 3', 'Cuadrante 1']), 'tier'] = 2
#a = asesores[(asesores['monetizacion'] > limite_3) & (asesores['cuadrante_actual'] == 'Cuadrante 1')].sort_values('monetizacion')

asesores = asesores[[
              'index',
              'region',
              'cuadrante_actual',
              'leads',
              'monetizacion',
              'tier',
    ]].sort_values(['cuadrante_actual', 'monetizacion'], ascending=[True, False]).copy()

#GET TIER 3
asesores['tier'].iloc[:5] = 3


asesores.to_excel(r'G:\.shortcut-targets-by-id\11jNJkPFSMwK6AvDuAdiz0e0pfqjRD2fl\01 Procesos\15 Procesos en construccion\04 Asignacion EC\preview.xlsx')



#TO SQL
engine = create_engine('mysql+pymysql://admin:D4t4**2020@54.86.65.139/ec')
asesores.rename(columns={
        'index': 'asesor',
        'monetizacion': 'tc_monetizacion'
    })[[
        'asesor',
        'cuadrante_actual',
        'leads',
        'region',
        'tc_monetizacion',
        'tier'
    ]].to_sql(
        'monetizacion_asesores',
        engine,
        if_exists='replace',
        index=False,
        chunksize=10000
    )

#Get productives asesor
asesores_tiers = gsuite.get_sheet_data('1KS0aC7HmemmLPqkqzFegQZufMT2_41hyyIHcCVKUtcI','Usuarios!A:K')
asesores_tiers.to_excel(r'G:\.shortcut-targets-by-id\11jNJkPFSMwK6AvDuAdiz0e0pfqjRD2fl\01 Procesos\15 Procesos en construccion\04 Asignacion EC\asesores.xlsx', index=False)
asesores_tiers = asesores_tiers[~ pd.isna(asesores_tiers['Apariciones Carrusel'])]



#UPDATE TIERS



#read tiers
tiers = pd.read_excel(r'G:\.shortcut-targets-by-id\11jNJkPFSMwK6AvDuAdiz0e0pfqjRD2fl\01 Procesos\15 Procesos en construccion\04 Asignacion EC\Asesores Tier.xlsm', sheet_name='Sheet1')
tiers = tiers.set_index('Alias')


#map
asesores_tiers['Apariciones Carrusel'] = asesores_tiers['Nombre corregido'].map(tiers['Tier'])
asesores_tiers['Apariciones Carrusel'] = asesores_tiers['Apariciones Carrusel'].replace(0,1).fillna(1).astype(int).astype(str)


#cargar usuarios a drive
creds = d2g.get_credentials()
gc = gspread.authorize(creds)
spreadsheet_key = '1KS0aC7HmemmLPqkqzFegQZufMT2_41hyyIHcCVKUtcI'       
d2g.upload(asesores_tiers, spreadsheet_key, 'Usuarios', credentials=creds, clean=True, col_names=True, row_names = False, df_size = True )


#read rules
reglas = magi_conn.read_yaml("./assignment/reglas_carrusel.yml")



#borrar asesores inactivos
asesores_delete = list(asesores_tiers['Correo del asesor'][asesores_tiers['Activo'] == '0'].drop_duplicates())
for asesor in asesores_delete:
    for region in reglas:       
        ix = 0
        for regla in reglas[region]:
            jx = 0
            for asesores_vivos in regla['asesores']:
                if asesores_vivos['email'] == asesor:
                    reglas[region][ix]['asesores'].pop(jx)
                    print(f'{asesor} eliminado!')
                else:
                    jx +=1
            ix +=1


for region in ['Internacional','ECL','Nacional','TLG']:
    region_name = "Región " + region
    asesores_reglas = reglas[region_name][0]['asesores']
    
    asesores_vivos = list(asesores_tiers['Correo del asesor']
                            [
                            (asesores_tiers['Activo'] == '1') & 
                            (asesores_tiers['Regla'] == region_name)
                            ].drop_duplicates())
    
    asesores_rules = []
    for x in asesores_reglas:
        asesores_rules.append(x['email'])
        
    asesores_delete = []
    for asesor in asesores_rules:
        if asesor not in asesores_vivos:
            asesores_delete.append(asesor)
        
    for asesor in asesores_delete:
        for regla in reglas[region_name]:
            jx = 0
            for asesores_vivos in regla['asesores']:
                if asesores_vivos['email'] == asesor:
                    reglas[region_name][0]['asesores'].pop(jx)
                    print(f'{asesor} eliminado!')
                else:
                    jx +=1
    


#add new asesors
for region in ['Internacional','ECL','Nacional','TLG']:
    region_name = "Región " + region
    
    region_current = reglas[region_name].copy()
    reglas.pop(region_name)
    
    asesores = []
    for i in region_current[0]['asesores']:
        asesores.append(i['email'])

    asesores = list(set(asesores))
    
    asesores_vivos = list(asesores_tiers['Correo del asesor']
                            [
                                (asesores_tiers['Activo'] == '1') & 
                                (asesores_tiers['Regla'] == region_name)
                            ].drop_duplicates())

    
    asesores_nuevos = []
    for asesor in asesores_vivos:
        if asesor not in asesores:
            asesores_nuevos.append(asesor)
            
            
    for email in asesores_nuevos:
            new_asesor = {
                'bag': 0,
                'current': 0,
                'dislike_program': [],
                'email': email,
                'like_program': [],
                'max': 9000
                }
            
            region_current[0]['asesores'].append(new_asesor)
            print(f"Se agrego a {email}")
  
    
    #reset next asesor
    if region_current[0]['next_asesor'] >= len(region_current[0]['asesores']):
        region_current[0]['next_asesor'] = 1
    
    #Add region
    reglas[region_name] = region_current



#Obtener los asesores por tiers
asesores_tiers_nacional = []

for ix, asesor in asesores_tiers.iterrows():
    asesor['Apariciones Carrusel'] = int(asesor['Apariciones Carrusel']) - 1 
    for j in range(asesor['Apariciones Carrusel']):
        if asesor['Regla'] == 'Región Nacional':
            asesores_tiers_nacional.append(asesor['Correo del asesor'])



#apply for regions
for region in ['Región Nacional']:
    region_current = reglas[region].copy()
    reglas.pop(region)
        
    
    asesores = []
    for i in region_current[0]['asesores']:
        asesores.append(i['email'])
        
    df = pd.DataFrame(asesores, columns = ['email']).reset_index()
    df['duplicated'] = df.duplicated(subset=['email'])
    indices = list(df['index'][df['duplicated']])
    
    ix = 0
    new_indices = []
    for ind in indices:
        new_indices.append(ind - ix)
        ix += 1
    
    for x in new_indices:
        region_current[0]['asesores'].pop(x)
    

    asesores_tiers_current = asesores_tiers_nacional
          
    
    for email in asesores_tiers_current:
            new_asesor = {
                'bag': 0,
                'current': 0,
                'dislike_program': [],
                'email': email,
                'like_program': [],
                'max': 9000
                }
            
            region_current[0]['asesores'].append(new_asesor)
    
    #reset next asesor
    if region_current[0]['next_asesor'] >= len(region_current[0]['asesores']):
        region_current[0]['next_asesor'] = 1
    
    #Add region
    reglas[region] = region_current



magi_conn.to_yaml(reglas, "./assignment/reglas_carrusel.yml")

magi_conn.to_yaml(reglas, r"G:\.shortcut-targets-by-id\11jNJkPFSMwK6AvDuAdiz0e0pfqjRD2fl\01 Procesos\07 cc_flows\tec-cc-flows\assignment\reglas_carrusel.yml")



import magi_core.magi_connections as conn
import assignment.carrusel as carrusel
import assignment.datasources as ds

import pandas as pd
import numpy as np
import random



def calculate_filter(filter, value, value_base, vector):
    if filter == 'es igual':
        vector.append(value == value_base)
    
    else:
        vector.append(value != value_base)
        
    return vector


def resolve_filter(determinante, no_determinante):
    if len(determinante) > 0 and len(no_determinante) > 0:
        return (False not in determinante) and (True in no_determinante)
    
    elif len(determinante) > 0:
        return (False not in determinante) 
    
    elif len(no_determinante) > 0:
        return (True in no_determinante)
    
    return False 
    

def get_region_probabilty():
    n = round(random.random(), 3)
    
    if n < .075:
        return "Región Internacional"
    if n < .11:
        return "Región TLG"
    else:
        return "Región Nacional" 
    
    
def assignment(base : pd.DataFrame, sf_leads : pd.DataFrame):
    #separate bases
    base_no_assignment = base[base['status'] != 'Contacto Nuevo'].copy()
    base = base[base['status'] == 'Contacto Nuevo'].copy()
    
    #get leads recidivist
    sf_leads = sf_leads[sf_leads['OwnerId'] != '0053f000000Fe2SAAS'].drop_duplicates('Email').set_index('Email')
    sf_leads['OwnerId'] = sf_leads['OwnerId'].apply(lambda row: row[:15])
    
    #get users
    users = conn.get_sf_users()
    users_r = users[users['Reincidentes'] == '1'].drop_duplicates("Id Salesforce").set_index("Id Salesforce")
    users_on = list(users['Correo del asesor'][(users['Activo'] == "1")])
    user_c = users[users['Activo'] == '1'].drop_duplicates("Correo del asesor").set_index("Correo del asesor")
    users_a = list(users['Correo del asesor'][(users['Recibe leads'] == "1") & (users['Activo'] == "1")])

    
    #get special rules
    rules = conn.get_special_rules()
    filters = conn.get_rule_filters()
    
    #calculate rule
    base['rule_name'] = None
    for rule in list(rules['rule_name'].unique()):
        filters_current = filters[filters['rule_name'] == rule].copy()
        
        base['filtros_determinantes'] = base.apply(lambda row: [], axis=1)
        base['filtros_no_determinantes'] = base.apply(lambda row: [], axis=1)

        for ix, f in filters_current.iterrows():
            if f['type_filter'] == 'Determinante':
                base['filtros_determinantes'] = base.apply(lambda row:  calculate_filter(f['filter'], f['value'], row[f['field']], row['filtros_determinantes']), axis=1)
            else:
                base['filtros_no_determinantes'] = base.apply(lambda row:  calculate_filter(f['filter'], f['value'], row[f['field']], row['filtros_no_determinantes']), axis=1)
        
        #get result
        base['res_filtros'] = base.apply(lambda row: resolve_filter(row['filtros_determinantes'], row['filtros_no_determinantes']),axis=1)
        
        base['rule_name'] = base.apply(lambda row: rule if row['res_filtros'] else row['rule_name'] ,axis=1)


    
    rules = rules.set_index('rule_name')
    base['owner'] = base['rule_name'].map(rules['owner'])
    base['owner_id'] = base['rule_name'].map(rules['owner_id'])
 
    

    del base['filtros_determinantes']
    del base['filtros_no_determinantes']
    del base['res_filtros']
    
    #separate direcrt assignment
    base_direct = base[~base['owner_id'].isin([np.nan, '', None])].copy()
    base_direct['assignment_type'] = 'Asignacion por regla especial'
    base_direct['validation_type'] = 'Lead no validado'
    base_direct['assignment_date'] = base_direct['processing_date']

    
    if len(base_direct) > 0:
        #print specials rule
        print("\n.........")
        print("Asignaciones especiales encontradas:")
        print(base['rule_name'].value_counts())
        
        
        
    base = base[base['owner_id'].isin([np.nan, '', None])].copy()
    # base['direct_repeat_lead'] = base['email_corrected'].isin(base_direct['email_corrected'])
    
    # base_direct_repeat = base[(base['direct_repeat_lead'])].copy()
    # base = base[(base['direct_repeat_lead'] == False)].copy()
    
    
    # #repeat in direct assignments
    # if len(base_direct_repeat) > 0:
    #     base_current = base_direct[['email_corrected','owner', 'queue_id']].drop_duplicates('email_corrected').set_index('email_corrected')           
    #     base_direct_repeat['owner'] = base_direct_repeat['email_corrected'].map(base_current['owner'])
    #     base_direct_repeat['assignment_type'] = 'Asignacion por reincidencia'
    #     base_direct_repeat['original_lead'] = 0
        
    #     base_direct = pd.concat([base_direct, base_direct_repeat], sort=False)
        
    #     del base_current
    
    
    
    #bases recidivism
    base['owner_id'] = base['email'].map(sf_leads['OwnerId'])
    base["owner"] = base['owner_id'].map(users_r['Correo del asesor'])
    
    #bases
    base_recidivism = base[~base['owner'].isin([np.nan])].copy()
    base_recidivism['assignment_type'] = 'Asignacion por reincidencia'
    base_recidivism['validation_type'] = 'Lead no validado'
    base_recidivism['assignment_date'] = base_recidivism['processing_date']
    base_recidivism['original_lead'] = 0
    

    base = base[base['owner'].isin([np.nan, '', None])]
    
    if len(base) > 0 :
        base['region_original'] = base['region']      
        base['region'] = 'Región Nacional'
        
        #base_assignment['region'] = base_assignment.apply(lambda row: "Región Arquitectura" if row['school'] == "Escuela de Arquitectura, Arte y Diseño" else row['region'], axis=1)
        base['region'] = base.apply(lambda row: 'Región ECL' if row['modality'] == 'En línea' else row['region'],axis=1)  
        base['region'] = base.apply(lambda row: 'Región TLG' if row['modality'] == 'Learning Gate' else row['region'],axis=1)
        base['region'] = base.apply(lambda row: 'Región Internacional' if row['es latam?'] else row['region'],axis=1)
        base['region'] = base.apply(lambda row: "Región EGOB" if row['school'] == "Escuela de Ciencias Sociales y Gobierno" else row['region'], axis=1)
        base['region'] = base.apply(lambda row: "Región EGADE" if (str(row['loaded_program']).find('(Executive Education)') > 0) else row['region'], axis=1)
        base['region'] = base.apply(lambda row: "Región Especial" if not pd.isna(row['rule_name']) else row['region'], axis=1)
        
        #get duplicates
        base['repeat_lead'] = base['email_corrected'].duplicated()
        
        base_repeat = base[(base['repeat_lead'])].copy()
        base = base[(base['repeat_lead'] == False)].copy()
        
        # assginment by probabilities
        base['assignment_type'] = 'Asignacion por carrusel'
        base_nacional = base[(base['region'] == 'Región Nacional') & (base['modality'].isin(['Live', 'Aula Virtual', 'Presencial']))].copy()
        base = base[~base['id'].isin(base_nacional['id'])].copy()
        
        random.seed(10)
        base_nacional['region'] = base_nacional['region'].apply(lambda x: get_region_probabilty() )
        
        base_nacional.loc[base_nacional.region != 'Región Nacional', 'assignment_type'] = 'Asignacion carrusel especial probabilidad'
        
        base = pd.concat([base, base_nacional], ignore_index=False, sort=False)
        del base_nacional



        #run assignment
        list_users = []
        reglas = ds.read_yaml("./assignment/reglas_carrusel.yml")
        
        hora = conn.get_datetime().hour
        
        if hora >= 8 and hora <= 20:
            users_asignator = conn.get_users_asignator()
        else:
            users_asignator = []
        
        
        if len(users_asignator) > 0:
            rule_15 = conn.get_users_15m()
            users['Activo Asignator'] = users['Correo del asesor'].isin(users_asignator)
            users['Inactivo por asignator'] = users['Correo del asesor'].isin(rule_15)
            users_asignator = list(users['Correo del asesor'][(users['Recibe leads'] == "1") & (users['Activo Asignator'] & (~users['Inactivo por asignator']))])
            users_a = list(users['Correo del asesor'][(users['Recibe leads'] == "1") & (users['Activo'] == "1") & (users['Activo Asignator'] == False)])
            
        
        list_users.append(users_asignator)    
        list_users.append(users_a)
        list_users.append(users_on)    
        
        base, reglas = carrusel.asignador_regional(base, reglas, list_users)
        ds.to_yaml(reglas, "./assignment/reglas_carrusel.yml")
           
        
        
        asesores = pd.DataFrame()
        for region in ['Región Nacional']:
            asesores_x = pd.DataFrame(reglas[region][0]['asesores'])
            asesores_x['repeat'] = asesores_x.apply(lambda row: list(asesores_x['email']).count(row['email']),axis=1)
            asesores_x = asesores_x[['email','repeat']][asesores_x['repeat'] > 1].drop_duplicates()
            
            asesores = pd.concat([asesores, asesores_x], ignore_index=True, sort=False)
        asesores = asesores.set_index('email')
        
        base['repeat_owner'] = base['owner'].map(asesores['repeat'])
        base['assignment_type'] = base.apply(lambda row: row['assignment_type'] + ' x' + str(row['repeat_owner']).replace('.0','') if (str(row['repeat_owner']) != 'nan') & (row['assignment_type'] == 'Asignacion por carrusel - offline') else row['assignment_type'], axis=1)


        #assignment repeats
        base_current = base[['email_corrected','owner', 'queue_id']].drop_duplicates('email_corrected').set_index('email_corrected')           
        base_repeat['owner'] = base_repeat['email_corrected'].map(base_current['owner'])
        base_repeat['queue_id'] = base_repeat['email_corrected'].map(base_current['queue_id'])
        base_repeat['assignment_type'] = 'Asignacion por reincidencia'
        base_repeat['original_lead'] = 0    

        #concat base and base_repeat
        base = pd.concat([base, base_repeat], ignore_index=True, sort=False)
        
        base["owner_id"] = base['owner'].map(user_c['Id Salesforce'])        
        base['region'] = base['region_original']
        base['validation_type'] = 'Lead no validado'
        base['assignment_date'] = base['processing_date']
        base['owner_assignment'] = base['owner']
        
        
    #return base
    base = pd.concat([base, base_no_assignment, base_recidivism, base_direct], ignore_index=True, sort=False)
    
    base["owner_region"] = base['owner'].map(user_c['Región'])
    base["owner_name"] = base['owner'].map(user_c['Nombre corregido'])
       
    
    return base.copy()
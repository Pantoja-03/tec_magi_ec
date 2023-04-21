import pandas as pd
from datetime import date

def add_comments(phone, valid_email, valid_phone, valid_program, valid_campus, status, test, email_black_list, phone_black_list):
    errors = []
    if not valid_email:
        errors.append("El email es invalido.")
        
    if not valid_phone:
        if len(phone) < 10:
           errors.append("El telefono tiene menos de 10 digitos.")
        else:
           errors.append("El telefono es invalido.")
        
    if not valid_program:
        errors.append("No encontramos el programa en nuestro catalogo.")
        
    if not valid_campus:
        errors.append("No encontramos el campus en nuestro catalogo.")
        
    if test:
        errors.append('El prospecto es considerado como una prueba')
        
    if email_black_list or phone_black_list:
        errors.append('El registro esta en la lista negra')
        
    if status == "Pendiente de Carga":
        errors.append('Este programa no se puede cargar a SF')
    if status == "Pendiente en SF":
        errors.append('Este lead no se pudo cargar a SF')
    
    if status == "Duplicado en SF":
        errors.append('El lead está duplicado en SF')
    if status == "Duplicado en Carga":
        errors.append('El lead está duplicado dentro de la Carga')
        
    return errors


def final_format(base: pd.DataFrame):
    base['comments'] = base.apply(lambda row: add_comments(
        row['phone'],
        row['valid_email'],
        row['valid_phone'],
        row['valid_program'],
        row['valid_campus'],
        row['status'],
        row['test'],
        row['email_black_list'],
        row['phone_black_list']
        ),axis=1)
    
    base.sort_values(['original_date'],ascending=[True] ,inplace=True)
    base['status'] = base['status'].replace('Contacto Nuevo', 'Cargado')
    base.loc[base.status == 'Cargado', 'upload_date'] = date.today()
    base = base.fillna("")
    
    base['remote_id'] = base['sf_id']
    base['owner_assignment'] = base['owner']
    base['status_assignment'] = base.apply(lambda row: 'Asignacion directa' if (row['owner'] != 'andres.pantoja@tec.mx') & (row['status'] == 'Cargado') else 'Pendiente de asignacion', axis=1)
    base['validate_cc'] = base.apply(lambda row: True if (row['owner'] == 'andres.pantoja@tec.mx')  else False, axis=1)
    
    
    base['comments'] = base['comments'].apply(lambda row: str(row).replace('[', '').replace(']','').replace("'",""))
    base['sf_source'] = base.apply(lambda row: row['supplier'] if (row['status'] == 'Cargado') & (str(row['supplier']) != "nan") else row['sf_source'],axis=1)
    base['sf_creation_date'] = base.apply(lambda row: row['processing_date'] if (row['status'] == 'Cargado') else '',axis=1)
    base['sf_status'] = base.apply(lambda row: "Contacto Nuevo" if (row['status'] == 'Cargado') else '',axis=1)
    base['sf_program'] = base.apply(lambda row: row['loaded_program'] if (row['status'] == 'Cargado') else '',axis=1)
    base['sf_owner'] = base.apply(lambda row: row['owner_assignment'] if (row['status'] == 'Cargado') else '',axis=1)   
    
    base['owner_modified'] = False
    
    
    base = base.astype(str)
        
    for col in ['upload_date','loaded_phone','loaded_campus','loaded_program', 'remote_id', 'status_assignment']:
        base[col] = base.apply(lambda row: '' if row['status'] != 'Cargado' else row[col], axis=1)
        
    
    return base.copy()
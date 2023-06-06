# -*- coding: utf-8 -*-
"""
Created on Mon May 29 18:43:47 2023

@author: segur
"""


from magi_core import magi_connections
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

programas = magi_connections.get_program_catalogs()
programas = programas.reset_index().drop_duplicates('Programa Salesforce')


reglas = magi_connections.get_special_rules()


filtros = magi_connections.get_rule_filters()
filtros = filtros[(filtros['rule_name'].isin(list(reglas['rule_name'].unique()))) & (filtros['field'] == 'loaded_program')]
filtros['esta en sf?'] = filtros['value'].isin(programas['Programa Salesforce'])


programas_invalidos = filtros[filtros['esta en sf?'] == False]

if len(programas_invalidos) > 0:
    msj_rule = ""
    for regla in programas_invalidos['rule_name'].unique():
            msj_programa = ""
            
            for programa in programas_invalidos[programas_invalidos['rule_name'] == regla]['value']:
                msj_programa += f"<ul><li>{programa}</li></ul>"
                           
            msj_rule += f"""<ul>
            <li>
            <p>{regla}:</p>
            {msj_programa}
            </li>
            </ul>
            """
            
    
    mensaje = f"""
        <p>Buenos dias equipo!</p>
        <p>Espero que estén teniendo un gran día. </p>
        <p>Les escribo para mantenerlos al tanto de los programas que pertenecen a una regla de asignacion especial pero que no identificamos dentro del listado de programas disponibles en Salesforce.</p>
        <br>
        <p>Aquí les comparto la lista:</p>
        {msj_rule}
        <br>
        <p>Si tienen alguna pregunta o duda sobre esta alerta diaria, no duden en contactarme estoy al pendiente.</p>
        <p>Gracias por su atención y colaboración!</p>
        <p>Saludos,</p>
    """.strip().replace('    ','')
    
    
    mailer = magi_connections.get_mailer()
    
    msg = MIMEMultipart()
    msg['From'] ='VEC ODT<vec_odt@servicios.tec.mx>'
    msg['To'] = ", ".join(['daniel.amezola@tec.mx', 'andres.pantoja@tec.mx'])
    msg['Cc'] = 'victor.tena@tec.mx'
    msg['Subject'] = "Alerta: Programas de reglas especiales no encontrados en Salesforce"
    msg.attach(MIMEText(mensaje, 'html'))
    mailer.send_message(msg)

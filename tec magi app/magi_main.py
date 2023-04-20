import pandas as pd
from sqlalchemy import create_engine
import requests
from datetime import datetime, timedelta
import logging
from platform import node

import magi.magi_core as magi_core


url_teams = 'https://tecmx.webhook.office.com/webhookb2/a3b7c10f-dcec-45ff-ba24-3169df7b7feb@c65a3ea6-0f7c-400b-8934-5a6dc1705645/IncomingWebhook/d3be11c1a39043889548bf81bfe0a219/300efe10-cba6-4d35-a017-9c4487310c45'


def init_log() -> logging.Logger:
    
    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger('Carga de datos UP4')

    handler = logging.FileHandler(r'../resources/logs/carga_datos_up4.log')
    logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)
    
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    log.addHandler(handler)
    return log


def get_base(status = 'created'):
    engine = create_engine('mysql://iup:$0aG3d3x@18.204.133.156/tecdap_iup')
    
    leads_iup = """
        SELECT * 
        FROM leads
       
        WHERE status = '{}'
    """.format(status)
    
    base = pd.read_sql(leads_iup, engine)
        
    base['original_date'] = base['created_at']
    
    print(f'Se van a procesar {len(base)} datos...')
    
    return base.copy()


#init log
log = init_log()


#get star time
start_time = datetime.now()

#get base to sf
print('Magi U4P...')
base = get_base()
base = base.fillna('')

#verify base
if len(base) > 0:
    #magi....
    base, res = magi_core.magi_I(base, use_bulk = False)

    #declare final time
    time_elapsed = datetime.now() - start_time 

    if res:
        msg = 'Duracion: {}'.format(time_elapsed) + '\n\n\t' + 'Se procesaron: ' + str(len(base)) + ' datos. \n\tSe asignaron: ' + str(len(base[(base['owner'] != '') & (base['owner'] != 'andres.pantoja@tec.mx')])) + ' datos.'
        #requests.post(url_teams, json={'text': msg})

    else:
        msg = 'Duracion: {}'.format(time_elapsed) + '\n\n\t' + 'No se pudo procesar la carga de datos. Error:\n\n\t' + str(res)
        requests.post(url_teams, json={'text': msg})
        
        # base.to_excel(rf'G:\Mi unidad\01 Procesos\03 Carga Leads\recorded\errors\carga_error_ec_{str(datetime.now()).replace(":","-")[:19]}.xlsx', index=False)
        
        # input(f"ERROR!!! \nAntes de cerrar la consola apaga la tarea programada.\n\n{res}\n")


    print("\n.........")
    print('Duracion: {}'.format(time_elapsed))
    print('Se procesaron: ' + str(len(base)) + ' datos.')
    print('Se asignaron: ' + str(len(base[(base['owner'] != '') & (base['owner'] != 'andres.pantoja@tec.mx')])) + ' datos.')

    print(".........\n")

    print('Gracias por usar el sistema magi para procesamiento y carga de datos')


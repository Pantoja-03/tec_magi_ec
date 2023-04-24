from magi_core import magi_main
from magi_core import magi_connections as magi_conn
from datetime import timedelta
import requests
import os
from dotenv import load_dotenv
import logging


load_dotenv()
url_teams = os.getenv('URL_TEAMS')



def init_log() -> logging.Logger:
    
    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger('Carga de datos UP4')

    handler = logging.FileHandler(r'./resources/logs/carga_datos_up4.log')
    
    logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)
    
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    log.addHandler(handler)
    return log


print('Magi U4P...')


#init log
log = init_log()

#get star time
start_time = magi_conn.get_datetime()


#start
try:
    log.info(f'Obteniendo leads a procesar - {magi_conn.get_datetime()}')
    base = magi_conn.get_iup_leads("Pendiente en SF' or (status = 'Cargado' and remote_id = '') or status = 'Invalido' or status = 'Procesando")
    base = base[(base['created_at'] > (magi_conn.get_datetime() - timedelta(days=2))) & (base['created_at'] < (magi_conn.get_datetime() - timedelta(minutes=10)))]
    base = base.fillna('')

    #verify base
    if len(base) > 0:
        #magi....
        base, res = magi_main.run_magi(base, use_bulk = False)

        #get end time
        time_elapsed = str( magi_conn.get_datetime() - start_time )[-8:]
        
        log.info(f'Duracion {time_elapsed} - {magi_conn.get_datetime()}') 
        
        if res == True:
            log.info(f'Se procesaron {len(base)} - {magi_conn.get_datetime()}')
            log.info(f'Se asignaron {len(base[(base["owner"] != "") & (base["owner"] != "andres.pantoja@tec.mx")])} datos - {magi_conn.get_datetime()}')            
            log.info(f'Proceso terminado de manera correcta - {magi_conn.get_datetime()}')

        else:
            msg = 'Duracion: {}'.format(time_elapsed)[-8:] + '\n\n\t' + 'No se pudo procesar la carga de datos. Error:\n\n\t' + str(res)
            requests.post(url_teams, json={'text': msg}) 

            msg = f'No se pudo procesar la carga de datos - {res}'
            log.error(f'{msg} -  {magi_conn.get_datetime()}')


except Exception as e:
    log.error(f'Ocurrio un error la obtener los leads de iup - {e} - {magi_conn.get_datetime()}')




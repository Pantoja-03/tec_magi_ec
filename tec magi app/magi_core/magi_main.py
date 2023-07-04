import pandas as pd
from magi_core import magi_connections as magi_conn
from magi_core import magi_validate
from magi_core import magi_assignment
from magi_core import magi_upload_sf
from magi_core import magi_final_format
from magi_core import magi_upload_iup
from magi_core import magi_update_assignment
from magi_core import magi_asignator
import logging


def init_log() -> logging.Logger:
    
    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger('Carga de datos UP4')

    handler = logging.FileHandler(rf'./resources/logs/carga_datos_up4_{magi_conn.get_datetime().date()}.log')
    
    logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)
    
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    log.addHandler(handler)
    return log

#init log
log = init_log()


def run_magi(base: pd.DataFrame, use_bulk = True, alert_teams = True):
    
    try:
        #magi validate
        log.info(f'Validando leads - {magi_conn.get_datetime()}')
        base, sf_leads = magi_validate.validate(base, alert_teams)

        
        #Verify status
        if len(base[base['status'] == 'Contacto Nuevo']) > 0:
            
            #magi assignment
            log.info(f'Aplicando reglas de asignacion leads - {magi_conn.get_datetime()}')
            base = magi_assignment.assignment(base, sf_leads)
    
    
            #magi upload sf
            log.info(f'VCargando leads - {magi_conn.get_datetime()}')
            base = magi_upload_sf.upload(base, use_bulk)


        #magi final format
        log.info(f'Aplicando formato final leads - {magi_conn.get_datetime()}')
        base = magi_final_format.final_format(base)


        #magi upload in iup
        log.info(f'Actualizando leads en iup - {magi_conn.get_datetime()}')
        magi_upload_iup.update_iup(base)

        
        #magi update assignment
        log.info(f'Actualizando la asignacion diaria - {magi_conn.get_datetime()}')
        magi_update_assignment.update_current_assignment(base)
        
        
        #send to asignator
        log.info(f'Enviando leads a asignator - {magi_conn.get_datetime()}')
        magi_asignator.send_to_asignator(base)
        
        return base.copy(), True
        

    except Exception as e:
        return base.copy(), e
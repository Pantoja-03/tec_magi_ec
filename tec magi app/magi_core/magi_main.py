import pandas as pd

from magi_core import magi_validate
from magi_core import magi_assignment
from magi_core import magi_upload_sf
from magi_core import magi_final_format
from magi_core import magi_upload_iup
from magi_core import magi_update_assignment
from magi_core import magi_asignator



def run_magi(base: pd.DataFrame, use_bulk = True):
    
    try:
        #magi validate
        base, sf_leads = magi_validate.validate(base)
        
        
        #Verify status
        if len(base[base['status'] == 'Contacto Nuevo']) > 0:
            
            #magi assignment
            base = magi_assignment.assignment(base, sf_leads)
    
    
            #magi upload sf
            base = magi_upload_sf.upload(base, use_bulk)


        #magi final format
        base = magi_final_format.final_format(base)


        #magi upload in iup
        magi_upload_iup.update_iup(base)

        
        #magi update assignment
        magi_update_assignment.update_assignment(base)
        

        #send to asignator
        magi_asignator.send_to_asignator(base)
        
        return base.copy(), True
        

    except Exception as e:
        return base.copy(), e
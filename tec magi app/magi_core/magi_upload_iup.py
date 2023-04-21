
import pandas as pd
from magi_core import magi_connections as magi_conn


def update_iup(base: pd.DataFrame):   
    base = base.fillna('')

    
    cols = [
        'processing_date',
        'upload_date',
        'status',
        'full_name',
        'email',
        'valid_email',
        'loaded_phone',
        'valid_phone',
        'loaded_campus',
        'valid_campus',
        'region',
        'corrected_program_name',
        'loaded_program',
        'valid_program',
        'modality',
        'program_type',
        'field_interest',
        'supplier',
        'owner',
        'owner_region',
        'queue_id',
        'valid_lead',
        'repeat_in_base',
        'original_lead',
        'comments',
        'remote_id',
        'sf_id',
        'sf_creation_date',
        'sf_status',
        'sf_source',
        'assignment_date',
        'assignment_type',
        'validation_type',
        'year_week',
        'owner_assignment',
        'origin_campaign',
        'owner_name',
        'validate_cc',
        'status_assignment',
        'sf_program',
        'sf_owner',
        'owner_modified',  
        'id'
    ]


    base_to_iup = base[cols]
    
    base_iup = []
    
    ix: int
    row: pd.Series
    for ix, row in base_to_iup.iterrows():
         base_iup.append(tuple(row))
        
    query = """
        UPDATE leads 
        SET 
            processing_date = %s,
            upload_date = %s,
            status = %s,
            full_name = %s,
            corrected_email = %s,
            valid_email = %s,
            loaded_phone = %s,
            valid_phone = %s,
            loaded_campus = %s,
            valid_campus = %s,
            region = %s,
            corrected_program_name = %s,
            loaded_program = %s,
            valid_program = %s,
            modality = %s,
            program_type = %s,
            field_interest = %s,
            supplier = %s,
            owner = %s,
            owner_region = %s,
            queue_id = %s,
            valid_lead = %s,
            repeat_in_base = %s,
            original_lead = %s,
            comments = %s,
            remote_id = %s,
            sf_id = %s,
            sf_creation_date = %s,
            sf_status = %s,
            sf_source = %s,
            assignment_date = %s,
            assignment_type = %s,
            validation_type = %s,
            year_week = %s,
            owner_assignment = %s,
            origin_campaign = %s,
            owner_name = %s,
            validate_cc = %s,
            status_assignment = %s,
            sf_program = %s,
            sf_owner = %s,
            owner_modified = %s 

        WHERE id = %s
    """

    conn = magi_conn.get_conn_db()

    cursor = conn.cursor()
    res = cursor.executemany(query, base_iup)
    conn.commit()
    cursor.close()
    conn.close()
    
    if res != len(base):
        print("Existieron " + str(len(base) - res) + " leads que no se pudieron actualizar en iup")
        
    print("Status actualizado en iup")
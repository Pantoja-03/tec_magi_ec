# -*- coding: utf-8 -*-
"""
Created on Tue Jul 20 21:40:12 2021

@author: segur
"""

import pandas as pd
from datetime import datetime
from magi_core import magi_connections as magi_conn



def update_current_assignment(base: pd.DataFrame):
    try:
        base_assignments = magi_conn.get_base_assignments()
            
        base['current'] = 0
        base['date'] = base['upload_date']
        assignments = base[base['status'] == 'Cargado'][['owner', 'date','current']].groupby(['owner', 'date']).count().reset_index()
        
        
        assignments['key'] = assignments['owner'].astype(str) + assignments['date'].astype(str)
        assignments['insert?'] = assignments['key'].isin(base_assignments['key'])
          
        assignments_insert = assignments[~assignments['insert?']].copy()
        assignments_update = assignments[assignments['insert?']].copy()
        
        if len(assignments_insert) > 0:
            assignments_insert['created_at'] = datetime.now().isoformat(' ', 'seconds')
            assignments_insert['updated_at'] = datetime.now().isoformat(' ', 'seconds')
            assignments_insert['daily_current'] = assignments_insert['current']
            
            to_assignment = assignments_insert[['created_at','updated_at','date','owner','daily_current']].copy()
            
            base_to_assignment = []
            
            for ix, row in to_assignment.iterrows():
                 base_to_assignment.append(tuple(row))
            
            query = """
                INSERT INTO assignments (
                    created_at,
                    updated_at,
                    date,
                    owner,
                    daily_current
                    )
                VALUES (
                    %s,
                    %s,
                    %s,
                    %s,
                    %s
                    )
            """
            
            conn = magi_conn.get_conn_db()
            cursor = conn.cursor()
            cursor.executemany(query, base_to_assignment)
            conn.commit()
            cursor.close()
            conn.close()
        
        
        if len(assignments_update) > 0:
            base_assignments['update'] = base_assignments['key'].isin(assignments_update['key'])
            assignments_update = assignments_update.set_index('key')
            base_assignments['current'] = base_assignments['key'].map(assignments_update['current'])
            
            base_assignments['daily_current'] = base_assignments['current'] + base_assignments['daily_current']
            base_assignments['updated_at'] = datetime.now().isoformat(' ', 'seconds')
            
            to_assignment = base_assignments[base_assignments['update']][['updated_at','daily_current', 'id']].copy()
            
            base_to_assignment = []
            
            for ix, row in to_assignment.iterrows():
                 base_to_assignment.append(tuple(row))
            
            query = """
                UPDATE assignments 
                SET
                    updated_at = %s,
                    daily_current = %s
                  
                WHERE id = %s
            """
        
            conn = magi_conn.get_conn_db()
            cursor = conn.cursor()
            cursor.executemany(query, base_to_assignment)
            conn.commit()
            cursor.close()
            conn.close()
        
        
        print("Asignacion actualizado en iup")

    except:
        pass
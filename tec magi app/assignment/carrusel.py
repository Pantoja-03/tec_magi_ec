#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pandas as pd

REGION_COL = "region"

class Carrusel:
    current = 0
    asesores = []

    def __init__(self, current, asesores, asesores_validos, asignator = False):
        self.current = current
        self.asesores = asesores
        self.asesores_validos = asesores_validos
        self.asignator = asignator

    def next(self):
        if len(self.asesores_validos) < 1 :
            return False, None
        
        asesor = self.asesores[self.current]["email"]
        res = False
        i = 0
                
        while not res:
            if asesor not in self.asesores_validos:
                self.current += 1
                
                if self.current >= (len(self.asesores)):
                     self.current = 0
                
                asesor = self.asesores[self.current]["email"]
                res = False
            
            else:
                res = True
            
            
            i += 1
            if i >= len(self.asesores):
                return False, None
        
        self.current += 1
        if self.current >= (len(self.asesores)):
             self.current = 0
              
        
        if self.asignator == True:
            assg = "Asignacion por carrusel - online"
            self.asesores_validos.remove(asesor)

        else:
            assg = "Asignacion por carrusel - offline"
        
        
        return asesor, assg
    
    


def asignador_regional(df: pd.DataFrame, reglas: dict, list_users = []):
    """retorna DataFrame con columna de Propietario
    Permite asignar los registros opcionalmente agregando restricciones por region.
    """
    # reglas = []
    df['owner'] = None
    df['queue_id'] = None
        
    for region in reglas:
        # Obtengo el extracto actual
        current_df: pd.DataFrame
        current_df = df[df[REGION_COL] == region].copy()
        # Recorro las colas de asignacion
        print("{region} [{n}]".format(region=region, n=len(current_df)))
        for queue in reglas[region]:
            # Leo los parametros ....
            iter_users = 0
            queue_id = queue['id']
            filters = queue['filters']
            asesores = queue['asesores']
            next_asesor = queue['next_asesor']

            # En caso de tener criterios los aplicamos
            iter_df = current_df
            for current_filter in filters:
                iter_df = iter_df[iter_df[current_filter]
                                  == filters[current_filter]]
                # print("{f} = {cf}".format(
                #     f=current_filter, cf=filters[current_filter]))
            #print("Queue {id} [{n}]".format(id=queue_id, n=len(iter_df)))

            if next_asesor >= len(asesores):
                next_asesor = 0
                
            carrusel = Carrusel(next_asesor, asesores, list_users[iter_users], True)
            
            
            for ix, row in iter_df.iterrows():
                owner, assignment_type = carrusel.next()

                while owner == False:   
                    try:
                        iter_users += 1
                        carrusel = Carrusel(next_asesor, asesores, list_users[iter_users], False)
                    except:
                        raise Exception(f"No hay asesores activos para la regla: {queue_id}")
                        
                    owner, assignment_type = carrusel.next()

                #guardo los datos del carrusel
                current_df.loc[ix, "owner"] = owner
                current_df.loc[ix, "assignment_type"] = assignment_type
                current_df.loc[ix, "queue_id"] = queue_id
            
            
            
            # Guardo el siguiente asesor
            new_regla = reglas[region][reglas[region].index(queue)]
            new_regla['next_asesor'] = carrusel.current
            #print(carrusel.current)
            new_regla['asesores'] = carrusel.asesores
            #current_df.to_excel(region + ".xlsx")
            df.loc[current_df.index, ['owner', 'queue_id', 'assignment_type']] = current_df
            
    return  (
        df,
        reglas,
    )

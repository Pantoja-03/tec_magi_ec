#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pandas as pd

REGION_COL = "region"

class Carrusel:
    current = 0
    asesores = []

    def __init__(self, current, asesores, asesores_validos):
        self.current = current
        self.asesores = asesores
        self.asesores_validos = asesores_validos

    def next(self, program):
        """retorna el correo del asesor
        Obtiene el siguiente correo del asesor en base a una asignacion secuencial
        """
        i = 0
        v = 0
        skip = False
        deduct_bag = False
        
        asesor = self.asesores[self.current]["email"]
        
        if (len(self.asesores_validos) > 0) and (asesor not in self.asesores_validos):
            self.current = self.current + 1
            
            if self.current == (len(self.asesores)):
                 self.current = 0
                
            #print(asesor)    
            return "Asesor no valido"
        
        
        dislike_program = self.asesores[self.current]["dislike_program"]
        bag = self.asesores[self.current]["bag"]
        
        i = self.current
        
        """
        Verificamos si el asesor puede aceptar el lead
        """
        while program in dislike_program:
            skip = True
            i = i + 1
            
            if i > (len(self.asesores) - 1):
                i = 0
            
            asesor = self.asesores[i]["email"]
            dislike_program = self.asesores[i]["dislike_program"]
            bag = self.asesores[i]["bag"]
        
        
        asesor_valid = asesor
        current_valid = i
        
        """
        Verificamos si el asesor tiene leads guardados para mantener el equilibrio
        """
        while v == 0 and bag > 0:
            deduct_bag = True
            skip = True
            
            i = i + 1
            
            if i > (len(self.asesores) - 1):
                i = 0
                
            asesor = self.asesores[i]["email"]
            dislike_program = self.asesores[i]["dislike_program"]
            bag = self.asesores[i]["bag"]
            
            if program in dislike_program:
                bag = 1
                           
            if asesor == asesor_valid:
                v = v + 1
                deduct_bag = False
        
        if skip:
            self.asesores[i]["current"] += 1
            #self.asesores[i]["bag"] += 1
            
            if deduct_bag:
                self.asesores[current_valid]["bag"] -= 1
                    
        else:
            self.asesores[self.current]["current"] += 1    
            
            if self.current == ( len(self.asesores) - 1):
                self.current = 0

            else:
                self.current = self.current + 1
    
        return asesor


def asignador_regional(df: pd.DataFrame, reglas: dict, asesores_validos = [], asesores_activos = []):
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
            carrusel = Carrusel(next_asesor, asesores, asesores_validos)
            
            ix: int
            row: pd.Series
            for ix, row in iter_df.iterrows():
                current_df.loc[ix, "owner"] = carrusel.next(row['loaded_program'])
                i = 0
                while current_df.loc[ix, "owner"] == "Asesor no valido":
                    current_df.loc[ix, "owner"] = carrusel.next(row['loaded_program'])
                    i = i + 1

                    if i >= 100:
                        print("No se pueden asignar asesores, verifique las reglas")
                        carrusel = Carrusel(next_asesor, asesores, asesores_activos)
                        i = 0
                
                current_df.loc[ix, "queue_id"] = queue_id
            
            # Guardo el siguiente asesor
            new_regla = reglas[region][reglas[region].index(queue)]
            new_regla['next_asesor'] = carrusel.current
            #print(carrusel.current)
            new_regla['asesores'] = carrusel.asesores
            #current_df.to_excel(region + ".xlsx")
            df.loc[current_df.index, ['owner', 'queue_id']] = current_df
            
    return  (
        df,
        reglas,
    )

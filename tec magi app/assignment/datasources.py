#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu May  7 19:02:20 2020

@author: daniel
"""
import yaml
from pathlib import Path
import gsuite_api.gsuite as gsuite
from datetime import date


def read_yaml(file_path: str):
    if not file_path.endswith('.yml'):
        file_path += '.yml'
    f = open(file_path, 'r', encoding='utf-8')
    res = yaml.load(f.read(), Loader=yaml.FullLoader)
    f.close()
    return res


def to_yaml(obj, file_path, snapshot=True):
    if not file_path.endswith('.yml'):
        file_path += '.yml'
    path = Path(file_path)
    # if snapshot and path.is_file():
    #     pass
    #     copyfile(
    #         path,
    #         "./assignment/snapshots/{name}-{time}.yml".format(
    #             name=path.name.replace('.yml', ''),
    #             time=str(datetime.now().timestamp())
    #         )
    #     )

    f = path.open('w', encoding='utf-8')
    yaml.dump(obj, f, encoding='utf-8', allow_unicode=True)
    f.close()


def read_valid_agents():
    #date_dic = str(date.today())

    agents = gsuite.get_sheet_data('1KS0aC7HmemmLPqkqzFegQZufMT2_41hyyIHcCVKUtcI', 'Usuarios!A:I')
    agents = agents.applymap(str)

    valid_agents = agents[(agents['Recibe leads'] == "1") & (agents['Activo'] == "1")]
    valid_agents = list(valid_agents['Correo del asesor'])

    return valid_agents




import json
import logging
import re

def extract_event_fields(event):
    """Extração dos campos do evento"""

    lista = []
    for key in event:
        if isinstance(event[key], dict):
            lista.extend([f'{key}_' + s for s in extract_event_fields(event[key])])
        else:
            lista.append(key)
    return lista
    
def extract_schema_fields(schema):
   """Extração dos campos do schema.json"""

    lista = []
    for key in schema['required']:
        if schema['properties'][key]['type'] == 'object':
            lista.extend([f'{key}_' + s for s in extract_schema_fields(schema['properties'][key])])
        else:
            lista.append(key)
    return lista

def extract_schema_types(schema):
    """Extração dos tipos das variáveis dado o valor no campo de exemplo"""

    dict_s = {}
    for key, value in schema['properties'].items():
        if value['type'] == 'object':
            dict_s.update({f'{key}_{k}': v for k, v in extract_schema_types(value).items()})
        else:
            dict_s.update({key: type(value['examples'][0])})
    return dict_s

def extract_event_types(event):
    """Extração dos tipos dos valores das variáveis"""
    
    dict_s = {}
    for key, value in event.items():
        if isinstance(event[key], dict):
            dict_s.update({f'{key}_{k}': v for k, v in extract_event_types(event[key]).items()})
        else:
            dict_s.update({key: type(value)})
    return dict_s

def extract_event_ids_from_log(path):
    """Extração dos ids de eventos que deram algum erro durante o processo, salvos no error.log"""
    
    with open(path, 'r') as file:
        log_content = file.read()

    event_ids = re.findall(r'event ([\w-]+) -', log_content)
    return set(event_ids)
    
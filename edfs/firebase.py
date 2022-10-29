import pandas as pd
import requests
import json

firebase_url = 'https://dsci551-project-52d43-default-rtdb.firebaseio.com/'

def seek(path):
    url = firebase_url + path + '.json'
    try:
        rget = requests.get(url)
        return rget
    except:
        print('ERROR')
        

def ls(path: str) -> str:
    '''List files under path/.

    Args:
        path (str): path starting from NameNode/
    Returns:
        (str) Success or Error message
    '''
    slist = seek(f"NameNode/{path}")
    rlist = slist.json()

    result_list = []

    if type(rlist) == dict: # iterate over rlist if rlist != None
        for key, value in rlist.items():
            if key == "_": # empty directory
                continue
            if type(value) == dict: # if item is folder, add '/' to the end of thge string
                result_list.append(key + "/")
            else:
                result_list.append(key)
        output = 'empty' if not result_list else ', '.join(result_list)
    elif not rlist:
        output = f'Path {path} not found'
    else: 
        output = f'{path} is not a folder'
    
    return output

        
def mkdir(path: str) -> str:
    '''Create directory if not exists

    Args:
        path: relative path to NameNode/
    Returns:
        (str) Success or Error message
    '''
    full_path = f'NameNode/{path}'
    if seek(full_path).json() is None:
        url = firebase_url + full_path + '.json'
        data = '{"_" : "_"}' # empty directory
        r = requests.put(url,data)
        output = f'Directory {path} created'
    else:
        output  = 'Directory ' + path + ' already exists'
    return output

        
def rm(path: str) -> str:
    '''Delete directory if exists

    Args:
        path: relative path to NameNode/
    Returns:
        (str) Success or Error message
    '''
    full_path = f'NameNode/{path}'
    if seek(full_path).json() is None:
        output = 'Directory not found'
    else:
        url = firebase_url + full_path + '.json'
        d = requests.delete(url)
        if d.status_code == 200:
            output = path + ' was succefully deleted'
    return output
            
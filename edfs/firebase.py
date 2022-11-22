import pandas as pd
import requests
import json
import re
import datetime
import os
import csv

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


def getPartitionLocation(file: str) -> str:
    '''Return the locations of partitions of the
        file
    Args:
        file (str): relative path to NameNode/
    Returns:
        (str) Success or Error message
    '''
    path = "NameNode/" + file + "/partitions"
    rpath = seek(path)
    partition = requests.get(rpath.url)
    pdict = partition.json()       

    if pdict is None:
        output = f'Partitions for {file} not found'
    else:
        output = json.dumps(pdict, indent=4, sort_keys=True) # Organizing the data
    
    return output


def readPartition(file, partition) -> str:
    '''Return the content of partition # of
    the specified file
    Args:
        file (str): relative path to NameNode/
        partition (str):  name of the partition
    Returns:
        (str) Success or Error message
    '''
    try:
        pdict = json.loads(getPartitionLocation(file))
        url = pdict[partition]
        pdict = requests.get(url).json()
        output = json.dumps(pdict, indent=4, sort_keys=True)
    except:
        output = 'Partition not found'
    return output


# cleans column names for firebase json object key
def varname (var):
    key = re.sub(r'[^A-Za-z0-9 ]+', '', var).replace(" ", "_")
    names = key if key != "" else "invalid_key"
    return names

def mtime():
#     to revert back
    #datetime.datetime.utcfromtimestamp(int(mtime)/1000).strftime('%Y-%-m-%-d %I:%M:%S') 
    return (datetime.datetime.now().timestamp()*1000)

def filesize(file): #file size in bytes
    return  os.path.getsize(file)

def indexing(dicts):
    dt = dict()
    for k,v in dicts.items():
        i = int(k.replace('p',''))
        dt[i] = v
    return dt

def record_partition(path, country, filename, url):
    try:
        npath = firebase_url + path + "/" + filename + "/partitions.json"
    #     print (npath ,":", url)
        mdata = {country : url}
        putMeta = requests.patch(npath, json.dumps(mdata))
        if putMeta.status_code == 400: print(country)
    #     print (putMeta)
    except:
        print (country)

def file_mdata(path, file, filename):
    npath = firebase_url + path + "/" + filename + ".json"
    mdata = {'ctime': mtime(),
             'name': file,
             'type': 'FILE',
             'filesize':filesize(file)}
    putMeta = requests.patch(npath, json.dumps(mdata))
    

# partition by Country (Original plan)
def put(file: str, path: str) -> str:
    filename = file.replace(".csv","")
    path = 'NameNode/' + path

 
    # creating dictinary to organize data into correct json format. 
    # added 'file name' to the dictionary to help differentiate data from different files
    dc = dict()
    with open(file, encoding = 'utf-8') as csvfile:
        csvReader = csv.reader(csvfile)
        
        for index, row in enumerate(csvReader):
            cname = varname(row[0])
            n = 'p' + str(index)
            if cname in dc:
                dc[cname][n] = (';'.join(row))
            else:
                dc[cname]={n:(';'.join(row))}
    
    if seek(path + '/' +filename).json() is None:
        for key, val in dc.items():
            url = firebase_url + 'DataNode/' + key + '/' + filename + '.json'
            putResponse = requests.put(url, json.dumps(val))
            if putResponse.status_code == 200:
                record_partition (path, key, filename, putResponse.url)
            else:
                print (file, 'failed to uploaded at partition', key)
        
        output =  file + ' was succesfully uploaded to ' + path
        
        file_mdata(path, file, filename)
        #add metadata information.
    else:
        output = file + " already exists in " + path
            
        
    return output
    

def cat(path):
    file = path.replace('.csv','')
    pdict = json.loads(getPartitionLocation(file))
    data = dict()
    for k,v in pdict.items():
        getPartition = requests.get(v).json()
        for key, val in getPartition.items():
            i = int(key.replace('p',''))
            data[i]=val.replace(';',',')
            
    ldata = list()
    for key in sorted(data):
        ldata.append(data[key])
    return ldata


def mapPartition(p, file):
    file_name = file.split('/')[-1]
    columns = f'https://dsci551-project-52d43-default-rtdb.firebaseio.com/DataNode/Country_Name/{file_name}.json'
    rlist =[ v for k, v in requests.get(columns).json().items()]
    readMap = indexing(requests.get(p).json())
    for key in sorted(readMap):
        rlist.append(readMap[key])
    return rlist 
    

# function to get year columns
def is_year (c):
    return any(char.isdigit() for char in c)    


def new_col(cols):
    new_col = list()
    for c in cols:
        if is_year(c):
            new_col.append(c[:4])
        else:
            new_col.append(c)
    return new_col


def to_df(data):
    df = pd.DataFrame(columns = data[0].split(';'), data=[row.split(';') for row in data[1:]])
    columns = new_col(df.columns.values)
    df.columns = columns
    df_melted = df.melt(id_vars=columns[:4], var_name='Year', value_name='Value')
    return df_melted


# function to get year columns
def is_year (c):
    return any(char.isdigit() for char in c)

def read_dataset(file: str):
    partitions = json.loads(getPartitionLocation(file))

    df_list = list()
    for country_name, dir in partitions.items():
        if country_name == 'Country_Name': # Store only column names. Ignore
            continue
        map = mapPartition(dir, file)
        df_list.append(to_df(map))
        break ### REMOVEE

    df = pd.concat(df_list)

    # change columns names
    new_columns = list()
    columns = df.columns
    for c in columns:
        if is_year(c):
            new_columns.append(c[:4])
        else:
            new_columns.append(c.replace(" ","_"))

    # change column names in dataframe
    df.columns = new_columns
    df['Year'] = df['Year'].astype(int)
    df = df.loc[df['Value'] != '..'].copy()
    df['Value'] = df['Value'].astype(float)
    return df


def test():
    return 'test1'
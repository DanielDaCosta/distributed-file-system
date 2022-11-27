import csv
import re
import sys
import pandas as pd
from pymongo import MongoClient
import simplejson as sp
import json
import os

#mongodb client setup
client = MongoClient('localhost', 27017)
db = client['edfs']
        
####################
# API Functions    #
####################

def read_dataset(path):
    df=pd.read_csv(path)
    df=df.reset_index()
    df_melted = df.melt(df.set_index('Country Name'))
    new_columns = list()
    columns = df_melted.columns.str.strip()
    for c in columns:
            new_columns.append(c.replace(" ","_"))

    # change column names in dataframe
    df_melted.columns = new_columns
    df_melted.columns = df_melted.columns.astype(str)
    #print("readDataset", df_melted.astype({'Year':'int', 'Value': 'float'}))
    return df_melted

def seek(path):
    #print("seek", db.blockLocations.find_one({"path": path}))
    return db.blockLocations.find_one({"path": path})

def mkdir(path, name):
    result = seek(path)
    output = ""
    #print("mkdir", result)
    if result:
        pathn = path + "/" + name
        db.blockLocations.insert_one({"path": pathn, "type" : 'DIRECTORY'})
        output = f"Directory created"
    else:
        output = f"Invalid path: {path}"
    #print("mkdir",output)
    return output

def rm(path, name):
    result = seek(path)
    path =  f"{path}/{name}"
    output = ""
    if result:
        db.blockLocations.insert_one({"Path": path, "type" : 'DIRECTORY'})
        output = f"{path} deleted"
    else:
        output = "invalid deletion"
    #print(output)
    return output


def ls(path):
    result = seek(path)
    print("ls",result)
    if result:
        if result["type"] == "FILE":
            output = "Cannot run 'ls' on files"
        elif result["type"] == "DIRECTORY":
            #output1=db.df.find()
            query = {"path": {"$regex": f"{path}","$options" :'i'}}
            output1=db.blockLocations.find(query)
            final=[]
            for o in output1:
                final.append(o)
    else:
        final = f"Invalid path: {path}"
    
    if(len(final)==0):
        return final
    else:
        return final

def readPartition(inp,file,path):
    #a=put(file,path)
    path1= '/'+path+'/'+file+'/'+inp
    X = db.blockLocations.find_one({"path":path1})
    if(X):
        return ("FILE EXISTS")
    else:
        return ("FILE DOES NOT EXIST")

def cat(path1):
    '''db.blockLocations.aggregate([
  { "$project": { "path": { "$concat": [ "$path", " - ", "$type" ] } } },
  { "$merge": "Concatenate" }
])'''
    #path=location=path1+file
    db.blockLocations.aggregate([
    { "$lookup":
        {
           "from": "df",
           "localField": "path",
           "foreignField": "location",
           "as": "merge"
        }
    },
    {
        "$merge":"Concatenate"
    }
])
    return(db.Concatenate.find_one({"path":path1}))
    return("Concatenated")

def put(path, name, csvf):
    header = [ "\ufeffCountry Name",	"Country Code",	"Indicator Name",
    	"2003",	"2004",	"2005",	"2006",	"2007",	"2008",	"2009",	
        "2010",	"2011",	"2012",	"2013",	"2014",	"2015",	"2016",
        "2017",	"2018",	"2019",	"2020",	"2021"]
    csvfile = open( csvf, 'r')
    reader = csv.DictReader( csvfile )
    #print(reader)
    for each in reader:
        #print(each)
        row={}
        blockLoc = {}
        for field in header:
            row[field]=each[field]
            blockLoc["path"] = path+ "/"+ each["\ufeffCountry Name"]
            row["location"] = path+ "/"+ each["\ufeffCountry Name"]
            blockLoc["type"] = "FILE"
        #print (row)
        db.df.insert_one(row)
        db.blockLocations.insert_one(blockLoc)
    return("Inserted Data")

#session = db.getMongo().startSession( { readPreference: { mode: "primary" } } )


######################
# Database Functions #
######################

def delete(list):
    try:
        for item in list:
            col=db[f"item"]
            col.drop()
        return "Dropped tables"
    except:
        return "Database drop error"

def new_env(edfs):
    try:
        db = client['edfs']
        db.blockLocations.insert_one({"path":'/root',"type":'DIRECTORY'})
        return f"{edfs} created"
    except:
        return "Database error"

def delete_env(edfs):
    try:
        client.drop_database('edfs')
    except:
        return "Database error"
    return  f"{edfs} deleted"

def start_env(edfs):
    try:
        client = MongoClient('localhost', 27017)
        db = client['edfs']
    except:
        return "Database error"
    return  f"{edfs} started"

def test_edfs(argv):

    edfs = "edfs"

    #Testing
    if "--delete" in argv:
        print(delete_env(edfs))
    elif "--new" in argv:
        print(new_env(edfs))
    elif "--restart" in argv:
        print(delete_env(edfs))
        print(new_env(edfs))
    else:
        print(start_env(edfs))
        print(mkdir("/root", "foo"))
        print(mkdir("/root/foo", "bar"))

test_edfs(sys.argv[1])
#test_edfs("--new")
print(mkdir('/root', "user"))
#mkdir("/root/foo", "bar")
#rm("/root/foo", "bar")
#put("/root/foo", "data", "/Users/digvijaydesai/Downloads/ashita_code/Data.csv")
#seek("/root/foo/data")
#seek("/root/foo/bar")
#rm("/root", "bar")
print(ls('/root/user'))
#print(cat("/root/foo/Argentina"))
#print(read_dataset("/Users/digvijaydesai/Downloads/ashita_code/Data.csv"))
#print(readPartition("XY","foo","root"))

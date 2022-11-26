import mysql.connector as ccnx
import csv
import re
import sys
import pandas as pd
from pymongo import MongoClient

########################
# mongodb client setup #
########################

client = MongoClient('localhost', 27017)
db = client['edfs']
#db=new Mongo().getDB("edfs")
#collection = db['name_of_collection']

#python connector setup
mydb = ccnx.connect(
    host="localhost",
    user="root",
    password="",
    database="edfs"
)
mycursor = mydb.cursor(buffered=True)

####################
# Helper Functions #
####################

def Sort_Tuple(tup: list, idx: int) -> list:
    return(sorted(tup, key = lambda x: x[idx]))

def key_idx(str_list):
    try:
        return str_list.index('Country Name')
    except:
        return 0

def key_cleaning(row):
    #key cleaning
    clean_key =  re.sub(r'[^A-Za-z0-9 ]+', '', row[0]).replace(" ", "_")
    key = clean_key if clean_key != "" else "invalid_key"
    if key.isnumeric() and key.length() > 0:
        key = f"t{key}"
    if len(key) >= 64:
        key = key[0:40]
    return key

####################
# API Functions    #
####################

def read_dataset(path):
    # (partition_name, csv_index, comma-separated-string)
    list_of_tuples = getPartitionData(path)
    list_of_lists = [tuple[2].split(",") for tuple in list_of_tuples]
    df = pd.DataFrame(list_of_lists)

    new_header = df.iloc[0] #grab the first row for the header
    df = df[1:] #take the data less the header row
    df.columns = new_header #set the header row as the df header

    df = df.drop(["Country Code", "Series Code"], "columns")

    df_melted = df.melt(id_vars=["Country Name", "Series Name"],
        var_name="Year",
        value_name="Value")

    df_melted["Year"] = df_melted["Year"].str[0:4]

    df_melted = df_melted.loc[df_melted.Value.str.isnumeric()].copy()

    # change columns names
    new_columns = list()
    columns = df_melted.columns
    for c in columns:
            new_columns.append(c.replace(" ","_"))

    # change column names in dataframe
    df_melted.columns = new_columns
    print("readDataset", df_melted.astype({'Year':'int', 'Value': 'float'}))
    return df_melted.astype({'Year':'int', 'Value': 'float'})

def seek(path):
    seek_statement = "SELECT * FROM df WHERE path = %s"
    mycursor.execute(seek_statement, (path,))
    #myresult = mycursor.fetchall()
    myresult={}
    for row in mycursor:
        myresult= {"path":row[0], "type":row[1]}
    #return myresult
    #print("seek", myresult)
    if myresult:
        #print("sdhg",myresult)
        return myresult

def mkdir(path, name):
    result = seek(path)
    output = ""
    #print("mkdir", result)
    if result:
        if result["type"] == "DIRECTORY":
            pathname = f"{path}/{name}"
            dup_result = seek(pathname)
            #collection = db[df]
            if not dup_result:
                insert_statement = "INSERT INTO df VALUES (%s, 'DIRECTORY')"
                mycursor.execute(insert_statement, (pathname,))
                db.df.insert_one(result)
                #mydb.commit()
                output = f"directory {name} created"
            else:
                output = "directory already exists"
    else:
        output = f"Invalid path: {path}"
    print("mkdir",output)
    #return output

def rm(path, name):
    result = seek(path)
    filepath =  f"{path}/{name}"
    if result:
        select_statement = "SELECT * FROM df WHERE path LIKE %s"
        mycursor.execute(select_statement, (filepath + "%",))
        result = mycursor.fetchall()
        row={}
        for i in mycursor:
            row= {"path":i[0], "type":i[1]}
            db.df.insert_one(row)
        if len(result) != 1:
            output = "invalid deletion"
        else:
            delete_statement = "DELETE FROM df WHERE path LIKE %s"
            mycursor.execute(delete_statement, (filepath,)) #TODO: adding % here will add -r functionality
            result=mycursor.fetchall()
            row={}
            for i in mycursor:
                row= {"path":i[0], "type":i[1]}
                db.df.insert_one(row)
            if(row):
                print(row)
            mydb.commit()
            output = f"{filepath} deleted"
    else:
        output = f"Invalid path: {filepath}"
    return output

def ls(path):
    result = seek(path)
    if result:
        if result["type"] == "FILE":
            output = "Cannot run 'ls' on files"
        elif result["type"] == "DIRECTORY":
            ls_statement = "SELECT * FROM df WHERE path REGEXP %s"
            mycursor.execute(ls_statement, (f"^{path}\/[^\/]+$",))
            output = mycursor.fetchall()
            row={}
            for i in output:
                row= {"path":i[0], "type":i[1]}
                db.df.insert_one(row)
            output1=db.df.find()
    else:
        output1 = f"Invalid path: {path}"
    for o in output1:
        print("ls", o)
    return output1

def getPartitionLocations(path):
    cat_statement = "SELECT * FROM blockLocations WHERE path = %s"
    mycursor.execute(cat_statement, (path,))
    result = mycursor.fetchall()
    row={}
    for i in result:
        row= {"path":i[0], "type":i[1]}
        db.df.insert_one(row)
    x=db.blockLocations.find()
    for j in x:
        print("getPartitionLocations(path):", j)
    return x

def readPartition(path, partition_name):
    mycursor.execute(f"SELECT * FROM {partition_name} WHERE path = %s", (path,))
    result = mycursor.fetchall()
    partition_data = []
    for line in result:
        partition_data.append((partition_name, line[1], line[2]))
    print("readPartition", partition_data)
    return partition_data

def cat(path):
    output = ""
    sorted_data_list = getPartitionData(path)
    for s in sorted_data_list:
        output += s[2] +"\n"
    print("Cat", output)
    return output

def getPartitionData(path):
    result = seek(path)
    output = ""
    if result:
        if result["type"] == "DIRECTORY":
            output = "Cannot run 'cat' on directories"
        elif result["type"] == "FILE":
            myresult = getPartitionLocations(path)
            data_list = []
            for partition in myresult:
                data_list = data_list + readPartition(path, partition[1])
            sorted_data_list = Sort_Tuple(data_list, 1)
            return sorted_data_list
    else:
        output = f"Invalid path: {path}"
    print("getPartitionData(path):", output)
    return output

def put(path, name, csv):
    result = seek(path)
    if result:
        if result["type"] == "DIRECTORY":
            dup_result = seek(f"{path}/{name}")
            if not dup_result:
                hash_lists = hash(path, name, csv)
                output = f"file {name} created"
            else:
                output = "file already exists"
        else:
            output = "cannot place a file in a file"
    else:
        output = f"Invalid path: {path}"
    print("put", output)
    return output


#session = db.getMongo().startSession( { readPreference: { mode: "primary" } } )

def hash(path, name, csv_file):
    #execute metadata alter
    doc={"path":f"{path}/{name}","type":'FILE'}
    #meta_statement = "INSERT INTO df VALUES (%s, 'FILE');"
    #mycursor.execute(meta_statement, (f"{path}/{name}",))
    #result = mycursor.fetchall()
    #mydb.commit()
    db.df.insert_one(doc)
    with open(csv_file) as f:

        key_list, csv_counter = {}, 0
        reader = csv.reader(f, delimiter=',')
        header = next(reader)
        key_index = key_idx(header)
        key = key_cleaning(header)

        #TODO: modularize this create code JFC
        try:
            #create_statement = f"""
             #   CREATE TABLE IF NOT EXISTS {key}(
              #      path varchar(255),
              #      data_index int,
              #      data text,
              #      FOREIGN KEY(path) REFERENCES df(path) ON DELETE CASCADE
              #  )"""
            #insert_hash_statement = f"INSERT INTO {key} VALUES (%s, %s, %s);"
            ##mycursor.execute(create_statement)
            #result=mycursor.fetchall()
            #mydb.commit()
            #mycursor.execute(insert_hash_statement, (path + "/" + name, csv_counter, ','.join(header)))
            #mydb.commit()
            f"db.getCollection({key})"
            #session.commitTransaction()
            row= {"path":path+"/"+name, "data_index":csv_counter,"data":','.join(header)}
            f"db.{key}.insert_one(row)"
            #f"db.{key}.insert_one(path+"/"+name,csv)"
            key_list[key] = None
            csv_counter += 1
        except:
            output = f"ERROR: {mycursor.statement}"

        for row in reader:
            key = key_cleaning(row)
            #try insert data into datanode
            try:
                #create_statement = f"""
                #    CREATE TABLE IF NOT EXISTS {key}(
                #        path varchar(255),
                #        data_index int,
                #        data text,
                #        FOREIGN KEY(path) REFERENCES df(path) ON DELETE CASCADE
                #    )"""
                #insert_hash_statement = f"INSERT INTO {key} VALUES (%s, %s, %s);"
                #mycursor.execute(create_statement)
                #result = mycursor.fetchall()
                #mydb.commit()
                #mycursor.execute(insert_hash_statement, (path + "/" + name, csv_counter, ','.join(row)))
                #result = mycursor.fetchall()
                #mydb.commit()
                f"db.getCollection({key})"
                #session.commitTransaction()
                r= {"path":path+"/"+name, "data_index":csv_counter,"data":','.join(row)}
                f"db.{key}.insert_one(r)"
                key_list[key] = None
                csv_counter += 1
            except:
                output = f"ERROR: {mycursor.statement}"
                rm(path, name)
                # return output

        #write data into datanodes
        for key in key_list.keys():
             #block_statement = "INSERT INTO blockLocations VALUES(%s, %s);"
             #mycursor.execute(block_statement, (f"{path}/{name}", key))
             row={"path":f"{path}/{name}","type":key}
             db.blockLocations.insert_one(row)
        mydb.commit()
        print("hash", key_list)
        return key_list

######################
# Database Functions #
######################

def delete(list):
    try:
        for item in list:
            drop_table = f"DROP TABLE {key}"
            mycursor.execute(drop_table)
            mydb.commit()
        return "Dropped tables"
    except:
        return "Database drop error"

def new_env(edfs):
    try:
        #f"CREATE DATABASE {edfs}"
        db=client['edfs']
        db.df.createCollection()
        row={"path":f"/root","type":f"DIRECTORY"}
        db.df.insert_one(row)
        #"INSERT INTO df VALUES ('/root', 'DIRECTORY')",
        db.blockLocations.createCollection()
        return f"{edfs} created"
    except:
        return "Database error"

def delete_env(edfs):
    try:
        db.edfs.dropDatabase()
    except:
        return "Database error"
    return  f"{edfs} deleted"

def start_env(edfs):
    try:
        f"use {edfs}"
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
        #todo: put check to make sure that the file source exists
        #print(put("/root/foo", "data", "../datasets/sql-edfs/data.csv"))
        #print(cat("/root/foo/data"))
        #print(ls("/root/foo"))
        #print(rm("/root", "data"))
        #print(ls("/tree"))

test_edfs(sys.argv[1])
mkdir("/root", "foo")
mkdir("/root/foo", "bar")
put("/root/foo", "data", "Data.csv")
#rm("/root", "bar")
#ls("/root/foo")

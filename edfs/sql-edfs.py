import mysql.connector as ccnx
import csv
import re
import sys
import sql_statements as ss
import argparse

#TODO: prepare statements
#TODO: grab data file from CLI
#TODO: listen from CLI
def seek(path):
    ss.seek_statement
    mycursor.execute(ss.seek_statement, (path,))
    myresult = mycursor.fetchall()
    return myresult

def mkdir(path, name):
    result = seek(path)
    if result:
        if result[0][1] == "DIRECTORY":
            pathname = "{}/{}".format(path, name)
            dup_result = seek(pathname)
            if not dup_result:
                mycursor.execute(ss.insert_statement, (pathname,))
                mydb.commit()
                output = "directory {} created".format(name)
            else:
                output = "directory already exists"
    else:
        output = "Invalid path: {path}".format(path=path)
    return output

def rm(path, name):
    result = seek(path)
    clause =  "{}/{}".format(path,name)
    if result:
        mycursor.execute(ss.select_statement, (clause + "%",))
        result = mycursor.fetchall()
        if len(result) != 1:
            output = "invalid deletion"
        else:
            mycursor.execute(ss.delete_statement, (clause,)) #TODO: adding % here will add -r functionality
            mydb.commit()
            output = "{c} deleted".format(c=clause)
    else:
        output = "Invalid path: {path}".format(path=clause)
    return output

def ls(path):
    result = seek(path)
    if result:
        if result[0][1] == "FILE":
            output = "Cannot run 'ls' on files"
        elif result[0][1] == "DIRECTORY":
            mycursor.execute(ss.ls_statement, ("^{}\/[^\/]+$".format(path),))
            output = mycursor.fetchall()
    else:
        output = "Invalid path: {path}".format(path=path)
    return output

def getPartitionLocations(path):
    mycursor.execute(ss.cat_statement, (path,))
    return mycursor.fetchall()

def Sort_Tuple(tup):
    # reverse = None (Sorts in Ascending order)
    # key is set to sort using second element of
    # sublist lambda has been used
    return(sorted(tup, key = lambda x: x[1]))

def cat(path):
    result = seek(path)
    if result:
        if result[0][1] == "DIRECTORY":
            output = "Cannot run 'cat' on directories"
        elif result[0][1] == "FILE":
            myresult = getPartitionLocations(path)
            data_list = []
            for partition in myresult:
                mycursor.execute(ss.grab_statement.format(partition[1]), (path,))
                data_list.append(mycursor.fetchall()[0])
            sorted_data_list = Sort_Tuple(data_list)
            for s in sorted_data_list:
                output = s[2]
            return result
    else:
        output = "Invalid path: {path}".format(path=clause)
    return output

def put(path, name, csv):
    result = seek(path)
    if result:
        if result[0][1] == "DIRECTORY":
            dup_result = seek("{}/{}".format(path,name))
            if not dup_result:
                hash_lists = hash(csv, path, name)
                output = "file {} created".format(name)
            else:
                output = "file already exists"
        else:
            output = "cannot place a file in a file"
    else:
        output = "Invalid path: {path}".format(path=path)
    return output

def key_idx(str_list):
    try:
        return str_list.index('Country Name')
    except:
        return 0

def hash(file, path, name):
    with open(file) as f:
        key_list = []
        csv_counter = 0
        reader = csv.reader(f, delimiter=',')
        key_index = key_idx(next(reader))

        #execute metadata alter
        mycursor.execute(ss.meta_statement, ("{}/{}".format(path,name),))
        mydb.commit()
        file_success = True

        for row in reader:
            #key cleaning
            clean_key =  re.sub(r'[^A-Za-z0-9 ]+', '', row[0]).replace(" ", "_")
            key = clean_key if clean_key != "" else "invalid_key"
            if key.isnumeric() and key.length() > 0:
                key = "t{key}".format(key=key)
            if len(key) >= 64:
                key = key[:-1]
            #try insert data into datanode
            try:
                mycursor.execute(ss.create_statement.format(key))
                mydb.commit()
                mycursor.execute(ss.insert_hash_statement.format(key), (path + "/" + name, csv_counter, ','.join(row)))
                mydb.commit()
                key_list.append(key)
                csv_counter += 1
            except:
                output = "ERROR: {}".format(mycursor.statement)
                print(output)
                rm(path, name)
                return []
        for key in key_list:
             mycursor.execute(ss.block_statement, ("{}/{}".format(path, name), key))
        mydb.commit()
        return key_list


def delete(list):
    for item in list:
        mycursor.execute(ss.drop_table.format(key))
        mydb.commit()

def new_env():
    for s in ss.env_statements:
        mycursor.execute(s)
    mydb.commit()

def delete_env():
    mycursor.execute(ss.drop_database)
    mydb.commit()

if __name__ == "__main__":

    #python connector setup
    mydb = ccnx.connect(
      host="localhost",
      user="root",
      password=sys.argv[1],
    )

    mycursor = mydb.cursor(buffered=True)


    #TODO: add argparse
    if len(sys.argv) >= 3 and sys.argv[2] == "--new":
        new_env()
    elif len(sys.argv) >= 3 and sys.argv[2] == "--delete":
        delete_env()
    else:
        mycursor.execute(ss.use_database)

    # path = sys.argv[2].split('/')
    # path = list(filter(None, path))

    #Test runs
    print(mkdir("/root/foo", "bar"))

    print(put("/root", "data", "../datasets/sql-data/Data.csv"))
    # cat("/root/data")

    print(rm("/root", "data"))

    print(ls("/tree"))

import mysql
import mysql.connector as ccnx
import sys
import csv
import textwrap
import pandas as pd
import re
import json
import pymongo
import os
import simplejson as sp

#TODO: cat, put
#TODO: put sql queries in functions
#TODO: prepare statements
#TODO: delete datablocks with rm
def seek(path):
    mycursor.execute("SELECT * FROM df WHERE path = '" + path + "';")
    myresult = mycursor.fetchall()
    #print("Seek", myresult)
    #mycol=myDb["categories"]
    #item_id = mycol.insert_one(myresult[0])
    return myresult

def mkdir(path, name):
    result = seek(path)
    #print("out", result)
    if(1):
        #for i in result:
        #    print(i)
            #print("res[i]",result[i])
        #    if(result[i]=='DIRECTORY'):# result and result[0][1] == "DIRECTORY":
        #        #print("runnging")
        dup_result = seek(path + "/" + name)
        if not dup_result:
            mycursor.execute("INSERT INTO df VALUES ('" + path + "/" + name + "', 'DIRECTORY');")
            mydb.commit()
                    ##making changes
            temp = seek(path + "/" + name)
            #print("after execution", temp)
            #print(temp[0])
            myDb=myclient[mongodb_dbname]
            mycol=myDb["categories"]
            item_id = mycol.insert_one(temp[0])
                    ##end changes
            msg="directory " + name + " created"
            return msg
        else:
            msg="directory already exists"
            return msg
    else:
        msg="invalid path"
        return msg

def rm (filepath,filename):
        if filepath=='/':
            print(query)
            query = filetypes.find({"Path":filepath+filename},{"Path":1})
        else:
            query = filetypes.find({"path": filepath+'/'+filename}, {"Path": 1})
            #print(query)
        for q in query:
            #print(q)
            # print(query)
            if query:
                myDb.deleteMany({})
            else:
                msg="file does not exsit"
                return msg

def ls(path):
    result = seek(path)
    if(0):# result[0][1] == "FILE":
        print("Cannot run 'ls' on files")
    elif(1):
        collections=myDb.list_collection_names()
        return collections

def func(file,path):
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    df=pd.read_csv(r'/Users/digvijaydesai/Downloads/Data.csv')
#     df=pd.read_csv(f"{path}'/'{datafile}")
    dfa = pd.DataFrame(df.columns.values) 
    z=dfa.T
    count=1
    mydb=myclient["df1"]
    x=df['Country Name'].values.tolist()
    path='root/'
    y=list(df.columns.values)
    for i in x: 
        bea = {'Country Name': i, 'Path': path+"/"+i}
        mypath=mydb.Path
        #print(bea)
        df1=pd.DataFrame()
        df2=pd.DataFrame()
        mycol=mydb.x[i]
        df1=df.iloc[count]
        df3=pd.concat([z,df1])
        df3.to_json(r'abc1.json', orient='records', lines=True)
        data1 = []
        y=list(df.columns.values)
        r=df.iloc[0].values
        d = dict(zip(y,r))
        with open("usc.json", "w")as outfile:
            sp.dump(bea, outfile,ignore_nan=True)
        with open("sample.json", "w")as outfile:
            sp.dump(d, outfile,ignore_nan=True)
        with open('sample.json') as file:
            file_data = json.load(file)
        with open('usc.json') as file:
            deez = json.load(file)
        mycol.insert_one(file_data)
        mypath.insert_one(deez)
        os.remove('sample.json')
        count=count+1
        if count==len(df):
            break
        arrayforcn=[]
        os.remove('usc.json')
        for x in mypath.find({},{"Country Name": 1}):
            arrayforcn.append(x)
        return arrayforcn

def getPartition(path):
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    df=pd.read_csv(r'/Users/digvijaydesai/Downloads/Data.csv')
    dfa = pd.DataFrame(df.columns.values) 
    z=dfa.T
    count=1
    mydb=myclient["mydatabase"]
    x=df['Country Name'].values.tolist()
    mypath=mydb.Path
    tmp = 1
    for i in x: 
        bea = {'Country Name': i, 'Path': path+"/"+i}
        print(bea)
        mycol=mydb["Country"+str(tmp)]
        mycol.insert_one(bea)
        df1=pd.DataFrame()
        df2=pd.DataFrame()
        mycol=mydb["Country"+str(tmp)]
        tmp += 1
        df1=df.iloc[count]
        df3=pd.concat([z,df1])
        df3.to_json(r'abc1.json', orient='records', lines=True)
#         print(df3)
        data1 = []
        r=df.iloc[0].values
        y=list(df.columns.values)
        d = dict(zip(y,r))
        with open("sample.json", "w")as outfile:
            sp.dump(d, outfile,ignore_nan=True)
        
        for line in open('sample.json', 'r'):
            data1.append(json.loads(line))   
            with open('sample.json') as file:
                file_data = json.load(file)
                #mycol.insert_one(file_data)
            #mycol.insert_many(bea)#mycol.insert_one(data1)
    
        os.remove('sample.json')
        
        count=count+1
        
        if count==len(df):
            break
        def insert(self, doc_or_docs, manipulate=True,safe=None, check_keys=True, continue_on_error=False, **kwargs):
            mypath.insert(bea)

def readPartition(inp,file,path):
    a=func(file,path)
#     print(a[])
    try:
        next(x for x in a if x["Country Name"] == inp)
        msg="File already exsists at this location")
        return msg
    except StopIteration:  
        msg="This file doesnt exsist"
        return msg
    

def Sort_Tuple(tup):
    # reverse = None (Sorts in Ascending order)
    # key is set to sort using second element of
    # sublist lambda has been used
    return(sorted(tup, key = lambda x: x[1]))

def cat(path):
    result = seek(path)
    print(result)
    if(0):#result[0][1] == "DIRECTORY":
        print("Cannot run 'cat' on directories")
    elif(1):#result[0][1] == "FILE":
        myresult = getPartition(path)
        data_list = []
        for partition in myresult:
            #grab_statement = "SELECT * FROM {table_name} WHERE path = %s".format(table_name=partition[1])
            a="""myDb.{table_name}.find({"$where": "this.path  == this. %s"})".format(table_name=partition[1]), (path,)"""
            data_list.append(a)
            return data_list
            
        sorted_data_list = Sort_Tuple(data_list)
        for s in sorted_data_list:
            print(s[2])
        return result #TODO actually flesh out cat here

def put(csv):
    df = pd.read_csv(csv)
    df.to_json(r'abc1.json', orient='records', lines=True)	
    data1 = []
    x = []
    ####
    x = 1
    for line in open('abc1.json', 'r'):
        #print(data1)
        x+=1
        #print("a")
        mycol=myDb["Country"+str(x)]
        mycol.insert_one(json.loads(line))
    ####
    """
    for line in open('abc1.json', 'r'):
        data1.append(json.loads(line))
        #print(data1)
        for a in data1:
            print("a", a)
            for x in range(len(data1)):
                mycol=myDb[str(x)]
                mycol.insert_one(a)"""
    #myDb.categories.insert_many(data1)
    #myDb.categories.create_index("CountryName")
    msg="data has been successfully entered"
    return msg

def key_idx(str_list):
    try:
        return str_list.index('Country Name')
    except:
        return 0


def delete(list):
    for item in list:
        myDb.edfs.drop()


#TODO:environment setup
if __name__ == "__main__":
    mydb = ccnx.connect(
    host="localhost",
    user= sys.argv[3],
    password=sys.argv[4]
    )
    mongodb_host="mongodb://localhost:27017/"
    mongodb_dbname="edfs"
    mycursor = mydb.cursor(dictionary=True)
    myclient=pymongo.MongoClient(mongodb_host)
    myDb=myclient[mongodb_dbname]
    mycol=myDb["df"]
    if len(sys.argv) >= 3 and sys.argv[1] == "--new":
        mycursor.execute("CREATE DATABASE edfs;")
        mycursor.execute("USE edfs;")
        mycursor.execute("CREATE TABLE df (path varchar(255), type varchar(255), PRIMARY KEY(path));")
        mycursor.execute("INSERT INTO df VALUES ('/root', 'DIRECTORY');")
        mycursor.execute("CREATE TABLE blockLocations (path varchar(255), partition_name varchar(255), CONSTRAINT FOREIGN KEY (path) REFERENCES df(path) ON DELETE CASCADE);")
        mydb.commit()
    elif len(sys.argv) >= 3 and sys.argv[1] == "--delete":
        myclient.drop_database('edfs')
        #mydb.commit()
    else:
        mycursor.execute("use edfs;")

    path = sys.argv[2].split('/')
    path = list(filter(None, path))
    #Test runs
    mkdir("/Users/digvijaydesai/Downloads", "Data.csv")
    mkdir("/Users/digvijaydesai/Downloads", "foo")
    mkdir("/Users/digvijaydesai/Downloads", "bar")
    put("/Users/digvijaydesai/Downloads/Data.csv")
    #func("/Users/digvijaydesai/Downloads/Data.csv")
    # cat("/root/data")
    #rm("/root", "data")
    #print(start_env(mycursor, edfs))
    #print(readPartition("Aruba","Data","/root"))
    #print(readPartition("Togo","Data","/root"))
    print(getPartition("/root"))
    print(mkdir("/root", "foo"))
    print(mkdir("/root/foo", "bar")) 
    #print (put("/root/foo", "data", "../Data.csv")) 
    #print(cat("/root/foo/data"))
    #print(ls("/root/foo" ))
    #print(rm("/root", "data"))
    #print(ls("/tree"))
    filetypes = myDb.filetypes
    contents = myDb.contents

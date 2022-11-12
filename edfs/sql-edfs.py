import mysql.connector as ccnx
import csv
import re
import sys

#TODO: prepare statements
#TODO: grab data file from CLI
#TODO: listen from CLI
def seek(path):
    seek_statement = "SELECT * FROM df WHERE path = %s"
    mycursor.execute(seek_statement, (path,))
    myresult = mycursor.fetchall()
    return myresult

def mkdir(path, name):
    result = seek(path)
    if result and result[0][1] == "DIRECTORY":
        pathname = "{}/{}".format(path, name)
        dup_result = seek(pathname)
        if not dup_result:
            insert_statement = "INSERT INTO df VALUES (%s, 'DIRECTORY')"
            mycursor.execute(insert_statement, (pathname,))
            mydb.commit()
            print("directory {} created".format(name))
        else:
            print("directory already exists")
    else:
        print("invalid path")

def rm(path, name):
    result = seek(path)
    select_statement = "SELECT * FROM df WHERE path LIKE %s"
    delete_statement = "DELETE FROM df WHERE path LIKE %s"
    clause =  "{}/{}".format(path,name)

    if result:
        mycursor.execute(select_statement, (clause + "%",))
        result = mycursor.fetchall()
        if len(result) != 1:
            print("invalid deletion")
        else:
            mycursor.execute(delete_statement, (clause,)) #TODO: adding % here will add -r functionality
            mydb.commit()
            print("{c} deleted".format(c=clause))
    else:
        print("Invalid path: {path}".format(path=clause))

def ls(path):
    result = seek(path)
    if result[0][1] == "FILE":
        print("Cannot run 'ls' on files")
    elif result[0][1] == "DIRECTORY":
        ls_statement = "SELECT * FROM df WHERE path REGEXP %s"
        mycursor.execute(ls_statement, ("^" + path + "\/[^\/]+$",))
        myresult = mycursor.fetchall()
        return myresult

def getPartitionLocations(path):
    cat_statement = "SELECT * FROM blockLocations WHERE path = %s"
    mycursor.execute(cat_statement, (path,))
    return mycursor.fetchall()

def Sort_Tuple(tup):
    # reverse = None (Sorts in Ascending order)
    # key is set to sort using second element of
    # sublist lambda has been used
    return(sorted(tup, key = lambda x: x[1]))

def cat(path):
    result = seek(path)
    if result[0][1] == "DIRECTORY":
        print("Cannot run 'cat' on directories")
    elif result[0][1] == "FILE":
        myresult = getPartitionLocations(path)
        data_list = []
        for partition in myresult:
            grab_statement = "SELECT * FROM {} WHERE path = %s".format(partition[1])
            mycursor.execute(grab_statement, (path,))
            data_list.append(mycursor.fetchall()[0])
        sorted_data_list = Sort_Tuple(data_list)
        for s in sorted_data_list:
            print(s[2])
        return result #TODO actually flesh out cat here

def put(path, name, csv):
    result = seek(path)
    if result and result[0][1] == "DIRECTORY":
        dup_result = seek("{}/{}".format(path,name))
        if not dup_result:
            hash_lists = hash(csv, path, name)
            print("file " + name + " created")
        else:
            print("file already exists")
    else:
        print("invalid path")

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
        meta_statement = "INSERT INTO df VALUES (%s, 'FILE');"
        mycursor.execute(meta_statement, ("{}/{}".format(path,name),))
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

            #allocate data node if not exists
            create_statement = """
                CREATE TABLE IF NOT EXISTS {}(
                    path varchar(255),
                    data_index int,
                    data text,
                    FOREIGN KEY(path) REFERENCES df(path) ON DELETE CASCADE
                )""".format(key)
            insert_statement = "INSERT INTO {} VALUES (%s, %s, %s);".format(key)

            #try insert data into datanode
            try:
                mycursor.execute(create_statement)
                mydb.commit()
                mycursor.execute(insert_statement, (path + "/" + name, csv_counter, ','.join(row)))
                mydb.commit()
                key_list.append(key)
                csv_counter += 1
            except:
                print("ERROR")
                print(mycursor.statement)
                rm(path, name)
                return []

        block_statement = "INSERT INTO blockLocations VALUES(%s, %s);"
        for key in key_list:
             mycursor.execute(block_statement, (path + "/" + name, key))
        mydb.commit()
        return key_list


def delete(list):
    for item in list:
        mycursor.execute("DROP TABLE " + key)
        mydb.commit()


if __name__ == "__main__":

    #python connector setup
    mydb = ccnx.connect(
      host="localhost",
      user="root",
      password=sys.argv[1],
    )

    mycursor = mydb.cursor(buffered=True)

    #TODO modularize into environment setup function
    if len(sys.argv) >= 3 and sys.argv[2] == "--new":
        #TODO: Prepare sql statements
        mycursor.execute("CREATE DATABASE edfs")
        mycursor.execute("USE edfs")
        mycursor.execute("""
            CREATE TABLE df (
                path varchar(255),
                type varchar(255),
                PRIMARY KEY(path)
            )""")
        mycursor.execute("INSERT INTO df VALUES ('/root', 'DIRECTORY')")
        mycursor.execute("""
            CREATE TABLE blockLocations (
                path varchar(255),
                partition_name varchar(255),
                CONSTRAINT FOREIGN KEY (path) REFERENCES df(path) ON DELETE CASCADE
            )""")
        mydb.commit()
    elif len(sys.argv) >= 3 and sys.argv[2] == "--delete":
        mycursor.execute("DROP DATABASE edfs;")
        mydb.commit()
    else:
        mycursor.execute("USE edfs;")

    # path = sys.argv[2].split('/')
    # path = list(filter(None, path))
    #Test runs
    mkdir("/root/foo", "bar")

    put("/root", "data", "../datasets/sql-data/Data.csv")
    # cat("/root/data")
    rm("/root", "data")

import mysql.connector as ccnx
import csv
import re
import sys
import pandas as pd


#python connector setup
mydb = ccnx.connect(
    host="localhost",
    user="root",
    password="root",
    database="edfs"
)
mycursor = mydb.cursor(buffered=True)

####################
# Helper Functions #
####################

def Sort_Tuple(tup: list, idx: int) -> list:
    '''
    Return the sorted list of tuples by the element of the tuple
    Args:
        tup (list): unsorted list of tuples
        idx (int): the element of the tuple to sort by
    Returns:
        (list) Sorted list of tuples
    '''
    return(sorted(tup, key = lambda x: x[idx]))

def key_idx(str_list):
    '''
    Return the index of 'Country Name' if it exists in the dataset, and 0 if
    the column name is not present
    Args:
        str_list (list): the list of column names
    Returns:
        (int) the index returned
    '''
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

    return df_melted.astype({'Year':'int', 'Value': 'float'})

def seek(path):
    '''
    Returns the filestructure that matches the specified path
    Args:
        str_list (list): the list of column names
    Returns:
        (int) the index returned
    '''
    seek_statement = "SELECT * FROM df WHERE path = %s"
    mycursor.execute(seek_statement, (path,))
    myresult = mycursor.fetchall()
    return myresult

def mkdir(path, name):
    '''
    Create the directory at the specfied path in the filesystem
    Args:
        path (str): the path to the directory's home
        name (str): the name of the directory
    Returns:
        output (str): success or failure of the operation
    '''
    result = seek(path)
    if result:
        if result[0][1] == "DIRECTORY":
            pathname = f"{path}/{name}"
            dup_result = seek(pathname)
            if not dup_result:
                insert_statement = "INSERT INTO df VALUES (%s, 'DIRECTORY')"
                mycursor.execute(insert_statement, (pathname,))
                mydb.commit()
                output = f"directory {name} created"
            else:
                output = "directory already exists"
    else:
        output = f"Invalid path: {path}"
    return output

def rm(path, name):
    '''
    Removes the directory at the specfied path in the filesystem
    Args:
        path (str): the path to the directory's home
        name (str): the name of the directory
    Returns:
        output (str): success or failure of the operation
    '''
    result = seek(path)
    filepath =  f"{path}/{name}"
    if result:
        select_statement = "SELECT * FROM df WHERE path LIKE %s"
        mycursor.execute(select_statement, (filepath + "%",))
        result = mycursor.fetchall()
        if len(result) != 1:
            output = "invalid deletion"
        else:
            delete_statement = "DELETE FROM df WHERE path LIKE %s"
            mycursor.execute(delete_statement, (filepath,)) #TODO: adding % here will add -r functionality
            mydb.commit()
            output = f"{filepath} deleted"
    else:
        output = f"Invalid path: {filepath}"
    return output

def ls(path):
    '''
    Returns the contents of the directory at the specfied path in the filesystem
    Args:
        path (str): the path to the directory's home
        name (str): the name of the directory
    Returns:
        output (str): success or failure of the operation
    '''
    result = seek(path)
    if result:
        if result[0][1] == "FILE":
            output = "Cannot run 'ls' on files"
        elif result[0][1] == "DIRECTORY":
            ls_statement = "SELECT * FROM df WHERE path REGEXP %s"
            mycursor.execute(ls_statement, (f"^{path}\/[^\/]+$",))
            output = mycursor.fetchall()
    else:
        output = f"Invalid path: {path}"
    return output

def getPartitionLocations(path):
    '''
    Returns the blockLocations that match the file at the specified
    Args:
        path (str): the path to the file
    Returns:
        output (obj): the blockLocations in a list of tuples
    '''
    cat_statement = "SELECT * FROM blockLocations WHERE path = %s"
    mycursor.execute(cat_statement, (path,))
    result = mycursor.fetchall()
    return result

def readPartition(path, partition_name):
    '''
    Returns the contents of a specified partition_name
    Args:
        path (str): the path to the file
        partition_name: the name of the partition
    Returns:
        [list of (tuple)]: the data contents of the partition
    '''
    mycursor.execute(f"SELECT * FROM {partition_name} WHERE path = %s", (path,))
    result = mycursor.fetchall()
    partition_data = []
    for line in result:
        partition_data.append((partition_name, line[1], line[2]))
    return partition_data

def cat(path):
    '''
    Returns the contents the file at the specified path
    Args:
        path (str): the path to the file
    Returns:
        output (obj): the text of the file
    '''
    output = ""
    sorted_data_list = getPartitionData(path)
    for s in sorted_data_list:
        output += s[2] +"\n"
    return output

def getPartitionData(path):
    '''
    Returns the contents the file at the specified path
    Args:
        path (str): the path to the file
    Returns:
        output (obj): the text of the file
    '''
    result = seek(path)
    output = ""
    if result:
        if result[0][1] == "DIRECTORY":
            output = "Cannot run 'cat' on directories"
        elif result[0][1] == "FILE":
            myresult = getPartitionLocations(path)
            data_list = []
            for partition in myresult:
                data_list = data_list + readPartition(path, partition[1])
            sorted_data_list = Sort_Tuple(data_list, 1)
            return sorted_data_list
    else:
        output = f"Invalid path: {path}"
    return output

def put(path, name, csv):
    '''
    places the file from a local directory into the EDFS
    Args:
        path (str): the path to the file
        name (str): the name of the file to be created
        csv (str): the path to the csv file to be placed
    Returns:
        output (str): the success or failure of the operation
    '''
    result = seek(path)
    if result:
        if result[0][1] == "DIRECTORY":
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
    return output



def hash(path, name, csv_file):
    '''
    Alters the metadata to allocate new datanotes if needed and places the file
    data into the nodes
    Args:
        path (str): the path to the file
        name (str): the name of the file to be created
        csv (str): the path to the csv file to be placed
    Returns:
        key_list (list): the list of keys to datanodes that have been
        allocated to
    '''

    #execute metadata alter
    meta_statement = "INSERT INTO df VALUES (%s, 'FILE');"
    mycursor.execute(meta_statement, (f"{path}/{name}",))
    mydb.commit()
    with open(csv_file) as f:

        key_list, csv_counter = {}, 0
        reader = csv.reader(f, delimiter=',')
        header = next(reader)
        key_index = key_idx(header)
        key = key_cleaning(header)

        #TODO: modularize this create code JFC
        try:
            create_statement = f"""
                CREATE TABLE IF NOT EXISTS {key}(
                    path varchar(255),
                    data_index int,
                    data text,
                    FOREIGN KEY(path) REFERENCES df(path) ON DELETE CASCADE
                )"""
            insert_hash_statement = f"INSERT INTO {key} VALUES (%s, %s, %s);"
            mycursor.execute(create_statement)
            mydb.commit()
            mycursor.execute(insert_hash_statement, (path + "/" + name, csv_counter, ','.join(header)))
            mydb.commit()
            key_list[key] = None
            csv_counter += 1
        except:
            output = f"ERROR: {mycursor.statement}"

        for row in reader:
            key = key_cleaning(row)
            #try insert data into datanode
            try:
                create_statement = f"""
                    CREATE TABLE IF NOT EXISTS {key}(
                        path varchar(255),
                        data_index int,
                        data text,
                        FOREIGN KEY(path) REFERENCES df(path) ON DELETE CASCADE
                    )"""
                insert_hash_statement = f"INSERT INTO {key} VALUES (%s, %s, %s);"
                mycursor.execute(create_statement)
                mydb.commit()
                mycursor.execute(insert_hash_statement, (path + "/" + name, csv_counter, ','.join(row)))
                mydb.commit()
                key_list[key] = None
                csv_counter += 1
            except:
                output = f"ERROR: {mycursor.statement}"
                rm(path, name)
                # return output

        #write data into datanodes
        for key in key_list.keys():
             block_statement = "INSERT INTO blockLocations VALUES(%s, %s);"
             mycursor.execute(block_statement, (f"{path}/{name}", key))
        mydb.commit()
        return key_list

######################
# Database Functions #
######################

def delete(list):
    '''
    Drops all tables in the list from the edfs
    Args:
        list (list): the list of table names to drop
    Returns:
        (str): the success or failure of the operation
    '''
    try:
        for item in list:
            drop_table = f"DROP TABLE {key}"
            mycursor.execute(drop_table)
            mydb.commit()
        return "Dropped tables"
    except:
        return "Database drop error"

def new_env(edfs):
    '''
    Executes EDFS SQL setup queries
    Args:
        edfs (str): the name of the EDFS filesystel to setup
    Returns:
        (str): the success or failure of the operation
    '''
    env_statements = [
            f"CREATE DATABASE {edfs}",
            f"USE {edfs}",
            """
                CREATE TABLE df (
                    path varchar(255),
                    type varchar(255),
                    PRIMARY KEY(path)
                )""",
            "INSERT INTO df VALUES ('/root', 'DIRECTORY')",
            """
                CREATE TABLE blockLocations (
                    path varchar(255),
                    partition_name varchar(255),
                    CONSTRAINT FOREIGN KEY (path) REFERENCES df(path) ON DELETE CASCADE
                )"""
    ]
    try:
        for s in env_statements:
            mycursor.execute(s)
        mydb.commit()
        return f"{edfs} created"
    except:
        return "Database error"

def delete_env(edfs):
    '''
    Drops the EDFS database entirely
    Args:
        edfs (str): the name of the EDFS filesystel to drop
    Returns:
        (str): the success or failure of the operation
    '''
    try:
        drop_database = f"DROP DATABASE {edfs};"
        mycursor.execute(drop_database)
        mydb.commit()
    except:
        return "Database error"
    return  f"{edfs} deleted"

def start_env(edfs):
    '''
    Uses EDFS database
    Args:
        edfs (str): the name of the EDFS filesystem to run
    Returns:
        (str): the success or failure of the operation
    '''
    try:
        use_database = f"USE {edfs}"
        mycursor.execute(use_database)
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
        print(put("/root/foo", "data", "../datasets/sql-edfs/data.csv"))
        print(cat("/root/foo/data"))
        print(ls("/root/foo"))
        print(rm("/root", "data"))
        print(ls("/tree"))


    # test_edfs(sys.argv)

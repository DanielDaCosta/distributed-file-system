import sys
sys.path.insert(1, '../edfs')
import sql
import firebase
import mysql.connector as ccnx
from enum import Enum

# class syntax
class EDFS(Enum):
    MYSQL = 1
    FIREBASE = 2
    MONGODB = 3

class FUNC(Enum):
    SUM = 1
    MAX = 2
    MIN = 3
    AVG = 4


#####################
#  Helper Function  #
#####################

def convert_str(item:str):
    try:
        return float(item)
    except:
        return None

#####################
#  Mapper Function  #
#####################

def mapPartition(key:str, col_data, data:str):
    """
        Arg: The name of the partition from getPartitionLocations
        col_data: the names of the columns
        data: the contents of the data itself
    """
    return None

def sql_map(mycursor, targets:[], file:str):
    sql.start_env(mycursor, "edfs")
    data = sql.getPartitionData(mycursor, file)
    print(len(data))
    print(data)
    # then I get all the partition locations and the indices and it goes zoooom

    #ID targets
    header = None
    header_list = (data[0][2].split(","))
    col_idxs = [header_list.index(i) for i in targets]

    #map step
    data_mapped = {}
    for d in data:
        partition_key, data_index, data_block = d[0], d[1], d[2].split(",")
        data_block = [(x, data_block[x]) for x in col_idxs]
        if partition_key not in data_mapped:
            data_mapped[partition_key] = []
        data_mapped[partition_key] += data_block
    return data_mapped

def edfs_shuffle(data_mapped:dict):
    data_shuffled = {}
    for key in data_mapped.keys():
        for item in data_mapped[key]:
            col_key, value = item[0], item[1]
            if col_key not in data_shuffled:
                data_shuffled[col_key] = []
            data_shuffled[col_key].append(convert_str(value))
    return data_shuffled

def edfs_reduce(data_shuffled:dict, function:int):
    data_reduced = {}
    if function == FUNC.SUM:
        for col in data_shuffled.keys():
            data_reduced[col] = sum(list(filter(lambda item: item is not None, data_shuffled[col])))
    elif function == FUNC.MAX:
        for col in data_shuffled.keys():
            data_reduced[col] = max(list(filter(lambda item: item is not None, data_shuffled[col])))
    elif function == FUNC.MIN:
        for col in data_shuffled.keys():
            data_reduced[col] = min(list(filter(lambda item: item is not None, data_shuffled[col])))
    elif function == FUNC.AVG:
        for col in data_shuffled.keys():
            shuffled_data = list(filter(lambda item: item is not None, data_shuffled[col]))
            data_reduced[col] = sum(shuffled_data)/len(shuffled_data)
    return data_reduced

def execute(mycursor, implementation:int, function:int, targets:[]=None, file:str=None, DEBUG=False):
    #TODO import getPartitionLocations() from each
    if implementation == EDFS.MYSQL:
        data_mapped = sql_map(mycursor, targets, file)
        data_shuffled = edfs_shuffle(data_mapped)
        data_reduced = edfs_reduce(data_shuffled, function)
        if DEBUG:
            print(f"Data Mapped:\n {data_mapped}\n")
            print(f"Data Shuffled:\n {data_shuffled}\n")
            print(f"Data Reduced:\n {data_reduced}\n")
        return data_reduced
    if implementation == EDFS.FIREBASE:
        data = firebase.cat(file)
        #TODO: firebase map
        return data

if __name__ == "__main__":
    if sys.argv[2] == "mysql":
        #python connector setup
        mydb = ccnx.connect(
          host="localhost",
          user="root",
          password=sys.argv[1],
        )
        mycursor = mydb.cursor(buffered=True)
        execute(mycursor, EDFS.MYSQL, FUNC.AVG, targets=["2021 [YR2021]", "2020 [YR2020]"], file="/root/foo/data", DEBUG=True)
        execute(mycursor, EDFS.FIREBASE, FUNC.AVG, targets=["2021 [YR2021]", "2020 [YR2020]"], file="root/user/Stats_Cap_Ind_Sample", DEBUG=True)

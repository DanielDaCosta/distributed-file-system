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

def intersection(lst1, lst2):
    return list(set(lst1) & set(lst2))

#####################
#  Mapper Function  #
#####################
def mongodb_map(targets, file):
    return mapPartition(placeholder1, placholder2)

def firebase_map(targets, file):
    data = firebase.cat(file)
    data = [row.split(",") for row in data] #list of list
    header_list = data[0]
    data = [(row[0], None, ','.join(row)) for row in data]
    targets = intersection(targets, header_list)
    col_dict = {header_list.index(i):i for i in targets}
    return mapPartition(data, col_dict)

def mapPartition(data, col_dict):
    """
        data: The data from the partitions
        col_dict: the names of the columns
    """
    data_mapped = {}
    for d in data:
        partition_key, data_block = d[0], d[2].split(",")
        data_block = [(col_dict[x], data_block[x]) for x in col_dict.keys()]
        if partition_key not in data_mapped:
            data_mapped[partition_key] = []
        data_mapped[partition_key] += data_block
    return data_mapped

def sql_map(mycursor, targets:[], file:str):
    sql.start_env(mycursor, "edfs")
    data = sql.getPartitionData(mycursor, file)
    # then I get all the partition locations and the indices and it goes zoooom

    header_list = (data[0][2].split(","))
    targets = intersection(targets, header_list)
    col_dict = {header_list.index(i):i for i in targets}
    return mapPartition(data, col_dict)

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
    if implementation == EDFS.MYSQL:
        data_mapped = sql_map(mycursor, targets, file)
    if implementation == EDFS.FIREBASE:
        data_mapped = firebase_map(targets, file)
    data_shuffled = edfs_shuffle(data_mapped)
    data_reduced = edfs_reduce(data_shuffled, function)
    for item in targets:
        if item not in data_reduced:
            data_reduced[item] = None

    if DEBUG:
        print(f"Data Mapped:\n {data_mapped}\n")
        print(f"Data Shuffled:\n {data_shuffled}\n")
        print(f"Data Reduced:\n {data_reduced}\n")

    return data_reduced

if __name__ == "__main__":
    if sys.argv[2] == "mysql":
        #python connector setup
        mydb = ccnx.connect(
          host="localhost",
          user="root",
          password=sys.argv[1],
        )
        mycursor = mydb.cursor(buffered=True)
        execute(mycursor, EDFS.MYSQL, FUNC.MIN, targets=["2019 [YR2019]", "2020 [YR2020]", "2023 [YR2023]"], file="/root/foo/data", DEBUG=True)
        # execute(mycursor, EDFS.FIREBASE, FUNC.AVG, targets=["2019 [YR2019]", "2020 [YR2020]"], file="root/user/Stats_Cap_Ind_Sample", DEBUG=True)

import sys
sys.path.insert(1, '../edfs')
import sql
import mysql.connector as ccnx
from enum import Enum

# class syntax
class EDFS(Enum):
    MYSQL = 1
    FIREBASE = 2
    MONGODB = 3

class FUNCTION(Enum):
    SUM = 1
    MAX = 2
    MIN = 3


#####################
#  Helper Function  #
#####################
def mapPartition(key:str, col_data, data:str):
    """
        Arg: The name of the partition from getPartitionLocations
        col_data: the names of the columns
        data: the contents of the data itself
    """
    return None

def sql_map(mycursor, targets, file="/root/foo/data"):
    sql.start_env(mycursor, "edfs")
    data = sql.getPartitionData(mycursor, file)
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

def sql_shuffle(data_mapped):
    data_shuffled = {}
    for key in data_mapped.keys():
        for item in data_mapped[key]:
            col_key, value = item[0], item[1]
            if col_key not in data_shuffled:
                data_shuffled[col_key] = []
            data_shuffled[col_key].append(value)
    return data_shuffled


def reduce(function, data):
    return None

def execute(mycursor, implementation:int, function:int, file:str=None, targets:[]=None):
    #TODO import getPartitionLocations() from each
    if implementation == EDFS.MYSQL:
        data_mapped = sql_map(mycursor, targets)
        data_shuffled = sql_shuffle(data_mapped)
        print(data_shuffled)
    # reduce(function, data)
    return None

if __name__ == "__main__":
    if sys.argv[2] == "mysql":
        #python connector setup
        mydb = ccnx.connect(
          host="localhost",
          user="root",
          password=sys.argv[1],
        )
        mycursor = mydb.cursor(buffered=True)
        execute(mycursor, EDFS.MYSQL, FUNCTION.SUM, targets=["2021 [YR2021]", "2020 [YR2020]"])

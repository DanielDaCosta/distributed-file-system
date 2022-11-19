import sys
sys.path.insert(1, '../edfs')
import sqledfs
import mysql.connector as ccnx
from enum import Enum

# class syntax
class EDFS(Enum):
    MYSQL = 1
    FIREBASE = 2
    MONGODB = 3

#TODO maybe push this to the sqledfs.py file?
def mapPartition(key:str, col_data, data:str):
    """
        Arg: The name of the partition from getPartitionLocations
        col_data: the names of the columns
        data: the contents of the data itself
    """
    return None

def sql_map(mycursor, file="/root/foo/data"):
    sqledfs.start_env(mycursor, "edfs")
    data = sqledfs.getPartitionData(mycursor, file)
    # then I get all the partition locations and the indices and it goes zoooom

    header = None
    for r in data:
        #grab targets and parse into partitions
        if r[1] == 0:
            header = r
        print(r)

    print(header)

def execute(mycursor, implementation:int, function:str=None, file:str=None, targets:[]=None):
    #TODO import getPartitionLocations() from each
    if implementation == EDFS.MYSQL:
        sql_map(mycursor)


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

        execute(mycursor, EDFS.MYSQL)

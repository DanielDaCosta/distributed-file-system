import sys
sys.path.insert(1, '../edfs')
import sqledfs
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

def execute(implementation:enum):
    #TODO import getPartitionLocations() from each
    return None

if __name__ == "__main__":

    if sys.argv[1] == "mysql":
        #python connector setup
        mydb = ccnx.connect(
          host="localhost",
          user="root",
          password="compla36080",
        )

        execute(EDFS.MYSQL)

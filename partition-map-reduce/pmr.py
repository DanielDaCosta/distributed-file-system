import sys
sys.path.insert(1, '../edfs')
import sqledfs

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

        execute(None)

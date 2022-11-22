import sys
sys.path.insert(1, '../edfs')
import firebase

if __name__ == "__main__":
    print(firebase.firebase_url)
    # print(firebase.read_dataset('https://dsci551-project-52d43-default-rtdb.firebaseio.com/DataNode/Zimbabwe/Stats_Cap_Ind_Sample.json'))
    print(firebase.cat("root/user/Stats_Cap_Ind_Sample"))

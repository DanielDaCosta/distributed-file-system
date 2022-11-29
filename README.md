# distributed-file-system
 Emulation-based system for distributed file storage and parallel computation

# Firebase
## EDFS Commands
- ls root/
- ls root/user/
- put root/user datasets/Consume_Price_Index.csv
- getPartitionLocation root/user/Consume_Price_Index
- readPartition root/user/Consume_Price_Index China
- readPartition root/user/Human_Capital_Index China
- cat root/user/Per_Capita_GDP
- getPartitionLocation root/user/Per_Capita_GDP


# MySQL
## Docker
```mysql -p```

## EDFS Commands
- mkdir /root user
- ls /root
- put /root/user Access_Electricity datasets/Access_Electricity.csv
- cat /root/user/Access_Electricity
- getPartitionLocations /root/user/Access_Electricity
- readPartition /root/user/Access_Electricity Haiti
- put /root/user Access_Fuels datasets/Access_Fuels.csv

# MongoDB

## Docker
```mongosh -u root```

## EDFS Commands
 - mkdir /root/foo
 - mkdir /root/foo bar
 - put /root/foo data datasets/Data.csv
 - cat /root/foo/data

# References
- https://realpython.com/python-dash/

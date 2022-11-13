block_statement = "INSERT INTO blockLocations VALUES(%s, %s);"
cat_statement = "SELECT * FROM blockLocations WHERE path = %s"
create_statement = """
    CREATE TABLE IF NOT EXISTS {}(
        path varchar(255),
        data_index int,
        data text,
        FOREIGN KEY(path) REFERENCES df(path) ON DELETE CASCADE
    )"""
delete_statement = "DELETE FROM df WHERE path LIKE %s"
drop_table = "DROP TABLE {}"
drop_database = "DROP DATABASE edfs;"
grab_statement = "SELECT * FROM {} WHERE path = %s"
insert_statement = "INSERT INTO df VALUES (%s, 'DIRECTORY')"
insert_hash_statement = "INSERT INTO {} VALUES (%s, %s, %s);"
ls_statement = "SELECT * FROM df WHERE path REGEXP %s"
meta_statement = "INSERT INTO df VALUES (%s, 'FILE');"
seek_statement = "SELECT * FROM df WHERE path = %s"
select_statement = "SELECT * FROM df WHERE path LIKE %s"
use_database = "USE edfs;"

env_statements = [
        "CREATE DATABASE edfs",
        "USE edfs",
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

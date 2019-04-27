import sqlite3
from sqlite3 import Error
def create_connection(db_file):
    """ create a database connection to a SQLite database """
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)    
    return None

def create_table(conn):
    """ create a table in a SQLite database """
    try:
        # command to create sql table
        create_table_sql =  """CREATE TABLE FUND(  GROUPNAME             TEXT     PRIMARY KEY     NOT NULL,
                                                   SUBMISSIONTYPE        TEXT     NOT NULL,
                                                   MONTHEND              TEXT     NOT NULL,
                                                   PROSPECTUS            TEXT
                                                );"""

        cursor = conn.cursor()
        cursor.execute(create_table_sql)
    except Error as e:
        print(e)

def create_fund(conn, fund):
    sql = ''' INSERT INTO FUND(GROUPNAME,SUBMISSIONTYPE,MONTHEND,PROSPECTUS)
              VALUES(?,?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, fund)
    return cur.lastrowid

test = create_connection("t.db")
create_table(test)
with test:
    fund1 = ('a','b','c','d')
    create_fund(test,fund1)
import sys

__author__ = 'arul'
import happybase

COLUMN_FAMILY_NAME='cf'

def createHbaseTable(tableName,host='localhost',port=9090,table_prefix='test'):
    con = happybase.Connection(host,port,table_prefix=table_prefix)
    if(not con.tables().__contains__(tableName)):
        con.create_table(tableName,{
            COLUMN_FAMILY_NAME:dict()
        })
        print "table Created"
    print con.tables()
    return con

def createTable(tableName):
    pass

#
# if __name__ == '__main__':
#     tableName = sys.argv[1]
#     createHbaseTable(tableName)



